from __future__ import annotations

import multiprocessing
import os
import signal
import time
import typing as tp

from hq.client import HQClient
from hq.types import TaskID, TaskStatus
from hq.util import generate_queue_name
from hq.worker import HQWorker, run

_TERMINAL_STATUSES = frozenset({"success", "error", "lost"})


def _run_worker(
    host: str,
    port: int,
    queue: str,
    verify: bool | str | None,
    fetch_n_tasks: int,
) -> None:
    # Own process group so HQExecutor can kill this worker and its nested
    # heartbeat/process children together (Python "daemon" children can otherwise
    # outlive a SIGTERM'd parent and keep polling the queue).
    os.setsid()
    worker = HQWorker(
        host=host,
        port=port,
        queue=queue,
        fetch_n_tasks=fetch_n_tasks,
        verify=verify,
    )
    run(worker)


class HQExecutor:
    """Orchestrate a single HQ workflow: queue, workers, submit, and wait."""

    def __init__(
        self,
        host: str = "http://localhost",
        port: int = 3000,
        *,
        queue: str | None = None,
        n_workers: int = 2,
        fetch_n_tasks: int = 3,
        verify: bool | str | None = None,
        manage_workers: bool = True,
    ) -> None:
        if n_workers < 0:
            raise ValueError(f"{n_workers=} must be >= 0")
        if fetch_n_tasks < 1:
            raise ValueError(f"{fetch_n_tasks=} must be >= 1")

        self.host = host
        self.port = port
        self.queue = queue or generate_queue_name()
        self.n_workers = n_workers
        self.fetch_n_tasks = fetch_n_tasks
        self.verify = verify
        self.manage_workers = manage_workers
        self.client = HQClient(
            host=host, port=port, queue=self.queue, verify=verify
        )
        self._worker_procs: list[multiprocessing.Process] = []
        self._active = False

    def __enter__(self) -> HQExecutor:
        if not self.client.ping():
            raise ConnectionError(
                f"Failed to connect to HQ server at {self.client.url}"
            )
        if self.manage_workers and self.n_workers > 0:
            self._start_workers()
        self._active = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: tp.Any,
    ) -> None:
        self._stop_workers()
        self._active = False

    def _require_active(self) -> None:
        if not self._active:
            raise RuntimeError("HQExecutor must be used as a context manager")

    def _start_workers(self) -> None:
        for _ in range(self.n_workers):
            proc = multiprocessing.Process(
                target=_run_worker,
                args=(
                    self.host,
                    self.port,
                    self.queue,
                    self.verify,
                    self.fetch_n_tasks,
                ),
                # Must be non-daemon: run() spawns heartbeat/process children.
                # Teardown uses killpg on the worker's process group instead.
                daemon=False,
            )
            proc.start()
            self._worker_procs.append(proc)

    def _stop_workers(self) -> None:
        """Tear down managed workers promptly (process group + SIGKILL)."""
        for proc in self._worker_procs:
            if not proc.is_alive() or proc.pid is None:
                continue
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except (ProcessLookupError, PermissionError, OSError):
                if proc.is_alive():
                    proc.terminate()
        for proc in self._worker_procs:
            proc.join(timeout=1)
        for proc in self._worker_procs:
            if not proc.is_alive() or proc.pid is None:
                continue
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError, OSError):
                if proc.is_alive():
                    proc.kill()
            proc.join(timeout=1)
        self._worker_procs.clear()

    def submit(
        self,
        fun: tp.Callable[[], tp.Any],
        *,
        name: str | None = None,
        queue: str | None = None,
    ) -> TaskID:
        self._require_active()
        return self.client.submit(fun, name=name, queue=queue)

    def map(
        self,
        fun: tp.Callable[[tp.Any], tp.Any],
        args: tp.Iterable[tp.Any],
        *,
        name: str | None = None,
        queue: str | None = None,
    ) -> list[TaskID]:
        self._require_active()
        return self.client.map(fun, args, name=name, queue=queue)

    def check(self, *task_ids: int) -> tuple[TaskStatus | None, ...]:
        self._require_active()
        return self.client.check(*task_ids)

    def wait(
        self,
        *task_ids: int,
        poll_interval: float = 3.0,
    ) -> tuple[TaskStatus | None, ...]:
        self._require_active()
        if len(task_ids) == 0:
            return tuple()

        ids = list(task_ids)
        while True:
            statuses = self.client.check(*ids)
            if all(self._is_terminal(status) for status in statuses):
                return statuses
            time.sleep(poll_interval)

    def map_and_wait(
        self,
        fun: tp.Callable[[tp.Any], tp.Any],
        args: tp.Iterable[tp.Any],
        *,
        name: str | None = None,
        queue: str | None = None,
        poll_interval: float = 3.0,
    ) -> tuple[TaskStatus | None, ...]:
        task_ids = self.map(fun, args, name=name, queue=queue)
        return self.wait(*task_ids, poll_interval=poll_interval)
    
    def gather(self, *task_ids: int) -> tuple[tp.Any, ...]:
        self._require_active()
        return self.client.gather(*task_ids)

    def wait_and_gather(
        self,
        *task_ids: int,
        poll_interval: float = 3.0,
    ) -> tuple[tp.Any, ...]:
        self._require_active()
        self.wait(*task_ids, poll_interval=poll_interval)
        return self.gather(*task_ids)

    @staticmethod
    def _is_terminal(status: TaskStatus | None) -> bool:
        if status is None:
            return True
        return status["status"] in _TERMINAL_STATUSES
