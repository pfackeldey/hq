from __future__ import annotations

import functools
import requests
import time

from hq.base import HQBaseConnection
from hq.util import deserialize_obj


# client extends with `fetch`
class HQWorker(HQBaseConnection):
    def _fetch_tasks(self, fetch_n: int = 1) -> list:
        if fetch_n < 1:
            raise ValueError(f"{fetch_n=} needs to be larger than zero")

        response = requests.get(f"{self.url}/tasks/{fetch_n}")
        response.raise_for_status()
        # pairs of task+heavy buf [[], ...]
        return response.json()["payloads"]

    def _process_task(self, payload: list) -> None:
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

    @classmethod
    def run_loop(cls, host: str, port: int, *, fetch_n: int = 1):
        # primitive while True worker loop
        while True:
            with cls(host, port) as worker:
                print(f"Trying to fetch {fetch_n} task(s)...")
                payloads = worker._fetch_tasks(fetch_n)
                if len(payloads) == 0:
                    print("No tasks currently exist, continue trying...")
                    time.sleep(1)
                    continue
                for payload in payloads:
                    result = worker._process_task(payload=payload)
                    print(f"Task {result=}")
            # let the server breathe
            time.sleep(0.1)
