from __future__ import annotations

import base64
import cloudpickle
import typing as tp
import uuid
import os
from pathlib import Path


def serialize_obj(obj: tp.Any) -> str:
    pck_obj = cloudpickle.dumps(obj)
    return base64.b64encode(pck_obj).decode("utf-8")


def deserialize_obj(obj: str | None) -> tp.Any:
    if obj is None:
        return None

    if not isinstance(obj, str):
        raise TypeError(f"{obj=} needs to be a string at this point")

    return cloudpickle.loads(base64.b64decode(obj.encode("utf-8"), validate=True))


def generate_queue_name() -> str:
    # uuid7 is 3.13+; coffea_env is often 3.12
    make_id = getattr(uuid, "uuid7", None) or uuid.uuid4
    return str(make_id())

def _result_root() -> Path:
    return Path(os.environ.get("HQ_RESULT_DIR", "/tmp/hq-results"))

def result_key(queue: str, task_id: int | str) -> str:
    return f"{queue}/{task_id}.pkl"

def result_path(queue: str, task_id: int | str) -> Path:
    return _result_root() / result_key(queue, task_id)

def resolve_result_path(locator: str) -> Path:
    """Accept relative key or absolute path from taskInfo.resultPath."""
    p = Path(locator)
    if p.is_absolute():
        return p
    return _result_root() / p

def load_result(locator: str) -> tp.Any:
    return deserialize_obj(resolve_result_path(locator).read_text())