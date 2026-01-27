from __future__ import annotations

import requests
import typing as tp

from hq.base import HQBaseConnection
from hq.util import serialize_obj, deserialize_obj

TaskID: tp.TypeAlias = int


class AddTaskDict(tp.TypedDict):
    task: str
    heavyKey: str | None


class TaskStatus(tp.TypedDict):
    status: tp.Literal["success"] | tp.Literal["running"] | tp.Literal["error"]
    info: str | None  # additional info, e.g. path to log file / error


# client extends with `submit` and `map`
class HQClient(HQBaseConnection):
    def submit(self, fun: tp.Callable[[], tp.Any]) -> TaskID:
        task = serialize_obj(fun)

        body = [AddTaskDict({"task": task, "heavyKey": None})]

        response = requests.post(f"{self.url}/tasks", json=body)
        if response.status_code != 200:
            raise Exception(f"Failed to submit task, got {response.status_code}")

        ids = response.json()["taskIds"]
        assert len(ids) == 1
        return ids[0]

    def map(
        self,
        fun: tp.Callable[[tp.Any], tp.Any],
        args: tp.Iterable[tp.Any],
        *,
        fun_name: str | None = None,
    ) -> tp.List[TaskID]:
        # First we serialize the fun and send it as the 'heavy' payload once
        # Then, we distribute the args each with a pointer to the heavy payload

        # heavy payload
        heavy = serialize_obj(fun)
        # is this sufficient/ok to use `id`?
        heavy_key = str(fun_name or id(fun))
        body = AddTaskDict({"task": heavy, "heavyKey": heavy_key})
        response = requests.post(f"{self.url}/heavy", json=body)
        if response.status_code != 200:
            raise Exception(f"Failed to pre-submit {fun}, got {response.status_code}")

        # submit tasks
        body = [
            AddTaskDict({"task": serialize_obj(arg), "heavyKey": heavy_key})
            for arg in args
        ]
        response = requests.post(f"{self.url}/tasks", json=body)
        if response.status_code != 200:
            raise Exception(
                f"Failed to submit tasks that map {fun} over {args}, got {response.status_code}"
            )

        return response.json()["taskIds"]

    def check(self, task_id: int) -> TaskStatus:
        response = requests.get(f"{self.url}/tasks/status/{task_id}")

        # raise if task doesn't exist
        response.raise_for_status()

        # else return status, the type is given by the server implementation (we can trust it)
        body = response.json()
        return TaskStatus(
            {"status": body["status"], "info": deserialize_obj(body["info"])}
        )
