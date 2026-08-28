#!/usr/bin/env bash
#
# testrun.sh — end-to-end smoke test for the whole hq stack over HTTPS.
#
# Brings up every moving part, runs an example workload through it, and tears
# everything down again:
#
#   redis  ->  bun queue server (TLS)  ->  worker(s)  ->  client (submits work)
#
# For now this always exercises the HTTPS boundary: it generates a self-signed
# certificate (if missing), starts the Bun server with HQ_SERVER_KEY_FILE /
# HQ_SERVER_CERT_FILE, and points the client + workers at https://localhost
# with HQ_VERIFY set to that cert.
#
# Usage (from the project root):
#   ./scripts/testrun.sh                 # run the "simple" example
#   EXAMPLE=dynamic ./scripts/testrun.sh # run the "dynamic" example
#   WORKERS=4 ./scripts/testrun.sh       # spawn 4 workers instead of the default 2
#
# Overridable knobs (with defaults):
#   EXAMPLE   simple        which example/<EXAMPLE>/{client,worker}.py to run
#   WORKERS   2             how many worker processes to spawn
#   HQ_PORT   3000          port the Bun server listens on
#   CERT_FILE cert.pem      TLS certificate path (created if absent)
#   KEY_FILE  key.pem       TLS private key path (created if absent)
#   REDIS_URL redis://localhost:6379
#   HQ_QUEUE  (generated)   shared tenant queue for workers + client (override to pin a name)

set -euo pipefail

# --- config ----------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

EXAMPLE="${EXAMPLE:-simple}"
WORKERS="${WORKERS:-2}"
HQ_PORT="${HQ_PORT:-3000}"
CERT_FILE="${CERT_FILE:-cert.pem}"
KEY_FILE="${KEY_FILE:-key.pem}"
REDIS_URL="${HQ_REDIS_URL:-redis://localhost:6379}"

HQ_HOST="https://localhost"
SERVER_URL="https://localhost:${HQ_PORT}"

CLIENT="example/${EXAMPLE}/client.py"
WORKER="example/${EXAMPLE}/worker.py"

LOG_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hq-testrun.XXXXXX")"
SERVER_LOG="${LOG_DIR}/server.log"
REDIS_LOG="${LOG_DIR}/redis.log"

# pids we may need to clean up; 0 means "not started by us"
REDIS_PID=0
SERVER_PID=0
WORKER_PIDS=()

# --- pretty logging --------------------------------------------------------
log()  { printf '\033[1;34m[testrun]\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m[ok]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[warn]\033[0m %s\n' "$*"; }
err()  { printf '\033[1;31m[error]\033[0m %s\n' "$*" >&2; }

# --- cleanup ---------------------------------------------------------------
cleanup() {
  local code=$?
  log "tearing down..."

  for pid in "${WORKER_PIDS[@]:-}"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  done
  [ "$SERVER_PID" -ne 0 ] && kill "$SERVER_PID" 2>/dev/null || true
  [ "$REDIS_PID"  -ne 0 ] && kill "$REDIS_PID"  2>/dev/null || true

  # give children a moment, then hard-kill any stragglers
  sleep 0.5
  for pid in "${WORKER_PIDS[@]:-}"; do
    [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null || true
  done
  [ "$SERVER_PID" -ne 0 ] && kill -9 "$SERVER_PID" 2>/dev/null || true
  [ "$REDIS_PID"  -ne 0 ] && kill -9 "$REDIS_PID"  2>/dev/null || true

  if [ "$code" -ne 0 ]; then
    err "run failed (exit $code). Logs kept at: $LOG_DIR"
  else
    log "logs at: $LOG_DIR"
  fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# --- prerequisite checks ---------------------------------------------------
need() { command -v "$1" >/dev/null 2>&1 || { err "missing required tool: $1"; exit 1; }; }
need bun
need uv
need curl

[ -f "$CLIENT" ] || { err "client not found: $CLIENT"; exit 1; }
[ -f "$WORKER" ] || { err "worker not found: $WORKER"; exit 1; }

# --- 1. TLS certificate ----------------------------------------------------
if [ -f "$CERT_FILE" ] && [ -f "$KEY_FILE" ]; then
  ok "using existing TLS cert ($CERT_FILE) and key ($KEY_FILE)"
else
  need openssl
  log "generating self-signed TLS cert for localhost..."
  openssl req -x509 -newkey rsa:4096 -nodes \
    -keyout "$KEY_FILE" -out "$CERT_FILE" -days 365 \
    -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" >/dev/null 2>&1
  ok "wrote $CERT_FILE and $KEY_FILE"
fi

# --- 2. redis --------------------------------------------------------------
# parse host:port out of REDIS_URL (redis://host:port)
redis_hostport="${REDIS_URL#redis://}"
redis_host="${redis_hostport%%:*}"
redis_port="${redis_hostport##*:}"

port_open() { (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null && exec 3>&- ; }

if port_open "$redis_host" "$redis_port"; then
  ok "redis already reachable at ${redis_host}:${redis_port}"
elif command -v redis-server >/dev/null 2>&1; then
  log "starting redis-server on port ${redis_port}..."
  redis-server --port "$redis_port" >"$REDIS_LOG" 2>&1 &
  REDIS_PID=$!
  for _ in $(seq 1 50); do
    port_open "$redis_host" "$redis_port" && break
    sleep 0.1
  done
  port_open "$redis_host" "$redis_port" || { err "redis failed to start (see $REDIS_LOG)"; exit 1; }
  ok "redis running (pid $REDIS_PID)"
else
  err "no redis reachable at ${redis_host}:${redis_port} and redis-server is not installed."
  err "start one yourself, e.g.: redis-server --port ${redis_port}"
  exit 1
fi

# --- 3. bun queue server (TLS) ---------------------------------------------
log "starting Bun queue server (HTTPS) on port ${HQ_PORT}..."
HQ_SERVER_PORT="$HQ_PORT" \
HQ_SERVER_KEY_FILE="$KEY_FILE" \
HQ_SERVER_CERT_FILE="$CERT_FILE" \
HQ_REDIS_URL="$REDIS_URL" \
  bun run typescript/server.ts >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

# wait until the /status endpoint answers over HTTPS
log "waiting for server to come up at ${SERVER_URL}/status ..."
server_ready=0
for _ in $(seq 1 100); do
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    err "server process died during startup (see $SERVER_LOG)"
    cat "$SERVER_LOG" >&2 || true
    exit 1
  fi
  if curl --cacert "$CERT_FILE" -fsS "${SERVER_URL}/status" >/dev/null 2>&1; then
    server_ready=1
    break
  fi
  sleep 0.1
done
[ "$server_ready" -eq 1 ] || { err "server did not become ready (see $SERVER_LOG)"; cat "$SERVER_LOG" >&2 || true; exit 1; }
ok "server is up and serving HTTPS (pid $SERVER_PID)"

# --- shared queue (tenant isolation) -----------------------------------------
# Workers start before the client, so the queue name must be chosen here and
# passed to every process via CLI args (see example/simple/client.py).
if [ -z "${HQ_QUEUE:-}" ]; then
  HQ_QUEUE="$(uv run python -c 'from hq.util import generate_queue_name; print(generate_queue_name())')"
fi
export HQ_QUEUE
log "using HQ_QUEUE=$HQ_QUEUE"

# --- 4. workers ------------------------------------------------------------
log "starting $WORKERS worker(s) against ${SERVER_URL} ..."
for i in $(seq 1 "$WORKERS"); do
  worker_log="${LOG_DIR}/worker-${i}.log"
  HQ_HOST="$HQ_HOST" HQ_PORT="$HQ_PORT" HQ_VERIFY="$CERT_FILE" \
    uv run "$WORKER" "$HQ_QUEUE" "$HQ_HOST" "$HQ_PORT" "$CERT_FILE" >"$worker_log" 2>&1 &
  WORKER_PIDS+=("$!")
  ok "worker $i started (pid ${WORKER_PIDS[-1]}, log $worker_log)"
done

# --- 5. client (submits the workload, blocks until tasks finish) -----------
log "running client: $CLIENT"
echo "----------------------------------------------------------------------"
if HQ_HOST="$HQ_HOST" HQ_PORT="$HQ_PORT" HQ_VERIFY="$CERT_FILE" \
     uv run "$CLIENT" "$HQ_QUEUE" "$HQ_HOST" "$HQ_PORT" "$CERT_FILE" 2>&1 | tee "${LOG_DIR}/client.log"; then
  client_rc=0
else
  client_rc=$?
fi
echo "----------------------------------------------------------------------"

if [ "$client_rc" -eq 0 ]; then
  ok "client finished successfully — end-to-end HTTPS run passed"
else
  err "client exited with code $client_rc"
  exit "$client_rc"
fi
