from __future__ import annotations

import base64
import cloudpickle
import typing as tp


def serialize_obj(obj: tp.Any) -> str:
    pck_obj = cloudpickle.dumps(obj)
    return base64.b64encode(pck_obj).decode("utf-8")


def deserialize_obj(obj: tp.Any) -> tp.Any:
    if obj is None:
        return None

    if not isinstance(obj, str):
        raise TypeError(f"{obj=} needs to be a string at this point")

    return cloudpickle.loads(base64.b64decode(obj.encode("utf-8"), validate=True))
