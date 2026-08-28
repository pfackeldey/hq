# hq (hep-queue)

A small **pull-based task queue** for distributing Python work — built to run
HEP analysis (coffea) chunks, but generic at its core.

Redis stores the work. A [Bun](https://bun.com) HTTP server is a thin facade
over Redis. Python clients submit cloudpickled callables; Python workers fetch
and run them, one subprocess per task. Results come back over a shared
filesystem or sent to the histserv server for histogramming.


## Quickstart

Uses [`bun`](https://bun.com) and [`uv`](https://docs.astral.sh/uv/).

1. Start Redis (or [dragonfly](https://github.com/dragonflydb/dragonfly), a
   drop-in replacement) at `redis://localhost:6379` (configurable via
   `HQ_REDIS_URL`):

```shell
redis-server --port 6379
```

2. Start the queue server:

```shell
bun run typescript/server.ts
```

3. Run work through it — the `HQExecutor` context manages a queue, local
   workers, and waiting in one block:

```python
from hq.executor import HQExecutor

def double(i: int) -> int:
    return i * 2

with HQExecutor(host="http://localhost", port=3000, n_workers=2) as ex:
    task_ids = ex.map(double, range(10))
    results = ex.wait_and_gather(*task_ids)   # (0, 2, 4, ..., 18)
```

Or drive the pieces manually (separate client and worker processes):

```shell
uv run example/simple/client.py            # submits tasks, prints the queue name
uv run example/simple/worker.py <queue>    # one or more workers, any machine
```

For an end-to-end smoke test of everything (including HTTPS), run
[`./scripts/testrun.sh`](scripts/testrun.sh).

## TLS (HTTPS)

The single HTTP boundary can be encrypted with a self-signed cert and two env
vars — no CA infrastructure:

```shell
openssl req -x509 -newkey rsa:4096 -nodes \
  -keyout key.pem -out cert.pem -days 365 \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

HQ_SERVER_KEY_FILE=key.pem HQ_SERVER_CERT_FILE=cert.pem bun run typescript/server.ts
```

Clients and workers take a `verify` argument (forwarded to `requests`):
`verify="cert.pem"` trusts that specific cert (dev), `verify=True` uses the
system CA bundle (real certs), `verify=False` disables verification (insecure,
dev only). Plain `http://` keeps working when TLS is not configured. Details:
[ADR 0003](https://github.com/ijohnkojo/hq_docs/blob/main/docs/adr/0003-tls-self-signed-certs.md)
and the
[deployment guide](https://github.com/ijohnkojo/hq_docs/blob/main/docs/ops/deployment.md).

## Running coffea on hq

`CoffeaHQExecutor` is a drop-in executor for `coffea.processor.Runner` —
one hq task per chunk, results merged with `processor.accumulate`:

```python
from hq.coffea import CoffeaHQExecutor

executor = CoffeaHQExecutor(
    host="https://localhost", port=3000, verify="cert.pem",
    n_workers=8,
    pickle_modules=(utils, ttbar_processor),  # ship notebook-local modules
)
```

See [CoffeaHQExecutor](https://github.com/ijohnkojo/hq_docs/blob/main/docs/architecture/coffea-executor.md)
— including why `pickle_modules` matters. A full AGC ttbar pipeline that
exercises hq end-to-end (and compares it against `FuturesExecutor`) lives in
the separate `agc-hq` repo, which installs hq as a package
(`pip install -e ../hq`).

Optional: stream histogram fills to
[histserv](https://github.com/scikit-hep/histserv) instead of returning pickled
hists over the shared filesystem — see
[histserv.md](https://github.com/ijohnkojo/hq_docs/blob/main/docs/architecture/histserv.md)
(`pip install 'hq[histserv]'`, then `USE_HISTSERV=True` in the AGC notebook).

## Documentation

Full documentation lives in a separate repo:
[ijohnkojo/hq_docs](https://github.com/ijohnkojo/hq_docs)
([index](https://github.com/ijohnkojo/hq_docs/blob/main/docs/index.md)).

| Section | Contents |
|---------|----------|
| [Architecture](https://github.com/ijohnkojo/hq_docs/blob/main/docs/architecture/overview.md) | System [overview](https://github.com/ijohnkojo/hq_docs/blob/main/docs/architecture/overview.md), [task lifecycle](https://github.com/ijohnkojo/hq_docs/blob/main/docs/architecture/task-lifecycle.md), [worker internals](https://github.com/ijohnkojo/hq_docs/blob/main/docs/architecture/worker.md), [results transport](https://github.com/ijohnkojo/hq_docs/blob/main/docs/architecture/results.md), [coffea executor](https://github.com/ijohnkojo/hq_docs/blob/main/docs/architecture/coffea-executor.md), [histserv](https://github.com/ijohnkojo/hq_docs/blob/main/docs/architecture/histserv.md) |
| [ADRs](https://github.com/ijohnkojo/hq_docs/blob/main/docs/index.md#architecture-decision-records) | Why pull-based, why an HTTP facade, TLS, shared-FS results, cloudpickle-by-value, subprocess-per-task, stderr IPC, worker teardown |
| [Operations](https://github.com/ijohnkojo/hq_docs/blob/main/docs/ops/deployment.md) | [Deployment](https://github.com/ijohnkojo/hq_docs/blob/main/docs/ops/deployment.md) (systemd, health checks, facilities), [configuration reference](https://github.com/ijohnkojo/hq_docs/blob/main/docs/ops/configuration.md), [troubleshooting](https://github.com/ijohnkojo/hq_docs/blob/main/docs/ops/troubleshooting.md) |

## Repository layout

| Path | Role |
|------|------|
| `src/hq/` | Python package: client, executor, worker, coffea integration, histserv helpers |
| `typescript/` | Bun queue server (routes, Redis state, TLS config) |
| `example/` | Simple examples + histserv smoke |
| `scripts/testrun.sh` | End-to-end HTTPS smoke test |
