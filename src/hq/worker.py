from __future__ import annotations

import functools
import multiprocessing
import requests
import time
import traceback
import socket
import os

from hq.base import HQBaseConnection
from hq.util import deserialize_obj, serialize_obj


# client extends with `fetch`
class HQWorker(HQBaseConnection):
    __slots__ = ("host", "port", "worker_id", "fetch_n_tasks")

    def __init__(
        self,
        host: str,
        port: int,
        worker_id: str
        | None = None,  # unique name, needs to be unique among all existing workers
        fetch_n_tasks: int = 1,  # number of tasks to fetch in a single API request
    ) -> None:
        super().__init__(host, port)
        if worker_id is None:
            self.worker_id = f"{socket.gethostname()}-{os.getpid()}"
        else:
            self.worker_id = worker_id
        if fetch_n_tasks < 1:
            raise ValueError(f"{fetch_n_tasks=} needs to be larger than zero")
        self.fetch_n_tasks = fetch_n_tasks

    def heartbeat(self) -> None:
        response = requests.get(f"{self.url}/status/{self.worker_id}")
        response.raise_for_status()

    def _fetch_tasks(self) -> dict:
        response = requests.get(
            f"{self.url}/tasks/fetch/{self.worker_id}/{self.fetch_n_tasks}"
        )
        response.raise_for_status()
        # pairs of taskIds and task+heavy buf [[], ...]
        return response.json()

    def _process_task(self, payload: list) -> None:
        assert len(payload) == 2, f"received unrecognisable {payload=}"
        task, heavy = payload

        task = deserialize_obj(task)
        heavy = deserialize_obj(heavy)

        # the default (task is a 0-arg callable)
        if heavy is None:
            assert callable(task), f"{task=} is not callable"
            del heavy
        # here: heavy is the callable and task the arg
        else:
            assert callable(heavy), f"{heavy=} is not callable"
            task = functools.partial(heavy, task)

        # run the task
        return task()


def _process_loop(worker: HQWorker) -> None:
    while True:
        with worker:
            print(f"Trying to fetch {worker.fetch_n_tasks} task(s)...")
            ids_and_payloads = worker._fetch_tasks()
            if len(ids_and_payloads) == 0:
                print("No tasks currently exist, continue trying...")
                continue

            ids = ids_and_payloads["taskIds"]
            payloads = ids_and_payloads["payloads"]
            for task_id, payload in zip(ids, payloads):
                try:
                    result = worker._process_task(payload=payload)
                    status = "success"
                    info = None
                except BaseException as error:
                    result = error
                    status = "error"
                    info = serialize_obj(error)

                # log the result
                if result is not None:
                    print(f"Task '{task_id}' finished with {status}: {result=}")

                # update task status in the queue, update 'info' in the future to e.g. include log file path or similar
                status_body = {
                    "workerId": worker.worker_id,
                    "taskStatus": {"status": status, "info": info},
                }
                response = requests.post(
                    f"{worker.url}/tasks/status/{task_id}", json=status_body
                )
                response.raise_for_status()
        # let the server breathe
        time.sleep(1)


def _heartbeat_loop(worker: HQWorker) -> None:
    while True:
        worker.heartbeat()
        time.sleep(1)  # ping every 1s


def run(worker: HQWorker) -> None:
    # start them as subprocesses to avoid delayed
    # heartbeats because of long-holding GIL in 'process'
    heartbeat = multiprocessing.Process(
        name="heartbeat", target=functools.partial(_heartbeat_loop, worker=worker)
    )
    heartbeat.start()

    process = multiprocessing.Process(
        name="process", target=functools.partial(_process_loop, worker=worker)
    )
    process.start()
