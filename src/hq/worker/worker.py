from __future__ import annotations

import json
import multiprocessing
from pathlib import Path
import requests
import time
import socket
import subprocess
import sys
import os
import tempfile
import typing as tp

from hq.base import HQBaseConnection
from hq.util import result_path, result_key


# worker extends with `fetch`
class HQWorker(HQBaseConnection):
    __slots__ = ("host", "port", "worker_id", "fetch_n_tasks", "queue", "verify")

    def __init__(
        self,
        host: str,
        port: int,
        *,
        worker_id: str | None = None,
        fetch_n_tasks: int = 1,
        queue: str,
        verify: bool | str | None = None,
    ) -> None:
        super().__init__(host, port, verify=verify)
        if worker_id is None:
            self.worker_id = f"{socket.gethostname()}-{os.getpid()}" # worker id is the hostname and pid, unless specified from the params
        else:
            if len(worker_id.strip()) == 0:
                raise ValueError(f"{worker_id=} can't be empty")
            self.worker_id = worker_id
        if fetch_n_tasks < 1:
            raise ValueError(f"{fetch_n_tasks=} needs to be larger than zero")
        self.fetch_n_tasks = fetch_n_tasks
        if len(queue.strip()) == 0:
            raise ValueError(f"{queue=} can't be empty")
        self.queue = queue.strip()

    def heartbeat(self) -> None:
        response = requests.get(
            f"{self.url}/status/{self.worker_id}", verify=self.verify # so essentially in this request we are sending the worker id to the server to record in the redis db, through the url  
        )
        response.raise_for_status()

    def _fetch_tasks(self) -> dict:
        response = requests.get(
            f"{self.url}/tasks/fetch/{self.worker_id}/{self.queue}/{self.fetch_n_tasks}",
            verify=self.verify,
        ) # so this response is a json object with taskIds and payloads
        response.raise_for_status()
        # pairs of taskIds and task+heavy buf [[], ...]
        return response.json()


def _parse_exe_ipc(err: str, *, task_id: int, returncode: int) -> dict:
    """exe.py prints IPC JSON as the last stderr line (warnings may precede it)."""
    lines = [ln for ln in err.splitlines() if ln.strip()]
    if not lines:
        raise RuntimeError(
            f"task {task_id} produced no stderr IPC (exit={returncode})"
        )
    try:
        return json.loads(lines[-1])
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"task {task_id} stderr IPC is not JSON (exit={returncode}): "
            f"{lines[-1][:200]!r}"
        ) from exc


def _process_loop(worker: HQWorker) -> None:
    while True:
        with worker:
            ids_and_payloads = worker._fetch_tasks()
            # Response is always a dict with keys; emptiness is taskIds, not len(dict).
            ids = ids_and_payloads.get("taskIds") or []
            payloads = ids_and_payloads.get("payloads") or []
            if len(ids) == 0:
                # Idle pull — stay quiet (long-lived Condor workers poll forever).
                time.sleep(1)
                continue

            print(f"Fetched {len(ids)} task(s) (asked for up to {worker.fetch_n_tasks})")

            # here we are iterating over the task ids and payloads, and for each task we are executing the task as a subprocess, and then updating the task status in the queue
            for task_id, payload in zip(ids, payloads):
                executable = Path(__file__).parent / "exe.py"
                # Pass payload via temp file to avoid OS ARG_MAX limits on large coffea closures
                with tempfile.NamedTemporaryFile(
                    mode="w",
                    suffix=".hq-payload.json",
                    delete=False,
                    encoding="utf-8",
                ) as tmp:
                    json.dump(payload, tmp)
                    payload_path = tmp.name
                try:
                    env = os.environ.copy()
                    # Ensure task subprocesses can import hq even if only sys.path
                    # was patched in the client (notebook) without PYTHONPATH.
                    src_root = str(Path(__file__).resolve().parents[2])
                    prev = env.get("PYTHONPATH", "")
                    if src_root not in prev.split(os.pathsep):
                        env["PYTHONPATH"] = (
                            src_root if not prev else src_root + os.pathsep + prev
                        )
                    proc = subprocess.Popen(
                        [
                            sys.executable,
                            str(executable),
                            str(task_id),
                            payload_path,
                        ],
                        stdout=None,
                        stderr=subprocess.PIPE,
                        text=True,
                        env=env,
                    )
                    _, err = proc.communicate()
                finally:
                    Path(payload_path).unlink(missing_ok=True)

                info = _parse_exe_ipc(err, task_id=task_id, returncode=proc.returncode)
                
                # heavy payload: local FS only, I need to strip it from the info before HTTP posting
                # Heavy payload stays on local FS — strip before HTTP status update
                task_result = info.pop("taskResult", None)
                if info.get("taskStatus") == "success" and task_result is not None:
                    out = result_path(worker.queue, task_id)
                    out.parent.mkdir(parents=True, exist_ok=True)
                    out.write_text(task_result)  # base64 from serialize_obj
                    info["taskInfo"]["resultPath"] = result_key(worker.queue, task_id)
                
                # update task status in the queue
                status_body = {
                    "workerId": worker.worker_id,
                    **info,  # taskStatus + taskInfo only — no taskResult
                }
                response = requests.post(
                    f"{worker.url}/tasks/status/{task_id}",
                    json=status_body,
                    verify=worker.verify,
                )
                response.raise_for_status()

        # let the server breathe
        time.sleep(1)


def _heartbeat_loop(worker: HQWorker) -> None:
    while True:
        worker.heartbeat()
        time.sleep(1)  # ping every 1s


# extend if needed, they're started as subprocesses
services: dict[str, tp.Callable[[HQWorker], None]] = {
    "heartbeat": _heartbeat_loop,
    "process": _process_loop,
}


def run(worker: HQWorker) -> None:
    service_procs = []
    for name, service in services.items():
        service_procs.append(
            multiprocessing.Process(
                name=name, target=service, args=(worker,), daemon=True
            )
        )

    for p in service_procs:
        p.start()

    for p in service_procs:
        p.join()
