"""Smoke test: ship a histserv RemoteHist through an hq task.

Proves that a RemoteHist handle survives cloudpickle into an hq worker
subprocess, that the worker can fill it over gRPC, and that the client sees
the merged bins via snapshot.

Requires running locally:
  redis-server --port 6379
  bun run typescript/server.ts        (hq server at http://localhost:3000)
  histserv --port 50051

Run:
  ~/anaconda3/envs/coffea_env/bin/python -u example/histserv_hq_smoke.py
"""

from __future__ import annotations

import numpy as np
from hist import Hist
from histserv import Client

from hq.executor import HQExecutor

HISTSERV_ADDRESS = "localhost:50051"
N_TASKS = 4
FILLS_PER_TASK = 1000


def make_fill_task(remote_hist, task_index: int):
    def fill() -> dict:
        rng = np.random.default_rng(task_index)
        remote_hist.fill(
            x=rng.normal(size=FILLS_PER_TASK),
            unique_id=("smoke", task_index),
        )
        return {"filled": FILLS_PER_TASK}

    return fill


def main() -> None:
    template = Hist.new.Reg(20, -4, 4, name="x").Double()

    with Client(address=HISTSERV_ADDRESS) as client:
        remote_hist = client.init(template)
        print(f"initialized: {remote_hist!r}")

        with HQExecutor(host="http://localhost", port=3000, n_workers=2) as ex:
            task_ids = [
                ex.submit(make_fill_task(remote_hist, i)) for i in range(N_TASKS)
            ]
            results = ex.wait_and_gather(*task_ids, poll_interval=1.0)
        print(f"task results: {results}")

        snapshot = remote_hist.snapshot(delete_from_server=True).to_hist()

    total = float(snapshot.sum(flow=True))
    expected = float(N_TASKS * FILLS_PER_TASK)
    assert total == expected, f"expected {expected} entries, got {total}"
    print(f"ok: snapshot holds {total:.0f} entries from {N_TASKS} hq tasks")


if __name__ == "__main__":
    main()
