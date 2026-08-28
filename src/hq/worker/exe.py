from __future__ import annotations

import functools
import json
import resource
import sys
import time
from pathlib import Path

# Fresh interpreter via Popen does not inherit the notebook/client sys.path.
# exe.py lives at <repo>/src/hq/worker/exe.py → parents[2] == <repo>/src
_SRC_ROOT = Path(__file__).resolve().parents[2]
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

from hq.util import serialize_obj, deserialize_obj


def _load_payload(payload_arg: str) -> list:
    """Payload is inline JSON, or a path to a JSON file (avoids ARG_MAX)."""
    if payload_arg.startswith("["):
        return json.loads(payload_arg)
    return json.loads(Path(payload_arg).read_text())


def main() -> None:
    # arg1: task_id, arg2: payload JSON or path to JSON file
    task_id, payload_arg = sys.argv[1:]
    task_id = int(task_id)

    payload = _load_payload(payload_arg)

    # Task deserialization (payload := [task, heavy]):
    # There are two options on how tasks are serialized:
    # 1. [task, None]: task is a 0-arg callable
    # 2. [task, heavy]: if heavy exists it is the 1-arg callable, and task is its argument
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

    # Task execution:
    # We try running the task and catch potential exceptions;
    # We also record the time it took to run it (to exclude deserialization time)
    # We return finally the taskStatus and taskInfo as JSON to the worker (parent process through stderr)
    start = time.time()
    try:
        result = task()
        info = {
            "taskStatus": "success",
            "taskInfo": {
                "runtime": time.time() - start,
                "peakRSS": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            },
            # IPC to parent only — parent writes this to the shared FS
            "taskResult": serialize_obj(result),
        }
    except BaseException as error:
        result = error
        info = {
            "taskStatus": "error",
            "taskInfo": {
                "runtime": time.time() - start,
                "peakRSS": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "errorType": type(error).__name__,
                "errorMessage": str(error),
            },
        }

    # Do not print full result — can be huge; warnings may also use stderr.
    print(
        f"Task {task_id} finished with '{info['taskStatus']}' "
        f"(took {info['taskInfo']['runtime']}s)"
    )

    # IPC to parent: last line of stderr must be this JSON object (warnings may precede it)
    print(json.dumps(info), file=sys.stderr)
    sys.stderr.flush()


if __name__ == "__main__":
    """
    Run this script to execute a hq payload as a subprocess, e.g.,

        $ python exe.py 1 '["...", "..."]'
        $ python exe.py 1 /tmp/payload.json

    where:
        arg1: task ID
        arg2: json serialized payload (2-element list of taskBuf & Optional[heavyBuf])
              or a filesystem path containing that JSON

    The idea of running the payload in a dedicated subprocess allows us to:
    - swap out the python executable (e.g. `uv run --with ... exe.py ...`)
    - source a custom env for this process (e.g. `source setup.sh && python exe.py '["foo", "bar"]' '1'`)

    This can be configured then _per-task_!

    It also allows for better debugging:
    One can execute this script manually and debug it.
    """
    main()
