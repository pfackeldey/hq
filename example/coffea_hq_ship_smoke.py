"""Smoke: ship nested callables + local modules to HQ workers (no ROOT).

Requires redis + TLS HQ server and:
  export HQ_RESULT_DIR=/tmp/hq-results

Run from repo root with coffea_env and PYTHONPATH=src (and example/ on path).
"""

from __future__ import annotations

from functools import partial
from pathlib import Path
import sys

from coffea.processor import accumulate

# example/ on sys.path so shipmod imports like notebook utils
_EXAMPLE_DIR = Path(__file__).resolve().parent
if str(_EXAMPLE_DIR) not in sys.path:
    sys.path.insert(0, str(_EXAMPLE_DIR))

import shipmod  # noqa: E402

from hq.coffea import CoffeaHQExecutor, register_modules_by_value  # noqa: E402

HOST = "https://localhost"
PORT = 3000
VERIFY = str(Path(__file__).resolve().parents[1] / "cert.pem")

ITEMS = list(range(10))


def _inner(i: int) -> dict[str, int]:
    # Requires worker task subprocess to run under coffea_env (sys.executable fix)
    import coffea

    _ = coffea.__version__
    return {"n": shipmod.scale(i), "count": 1, "coffea": 1}


def _retries_like(fn, item):
    """Minimal stand-in for coffea Runner's nested partial(automatic_retries, …)."""
    return fn(item)


if __name__ == "__main__":
    register_modules_by_value(shipmod)
    work_fn = partial(_retries_like, _inner)

    seed: dict[str, int] = {}
    expected = accumulate([work_fn(i) for i in ITEMS], seed)

    executor = CoffeaHQExecutor(
        host=HOST,
        port=PORT,
        verify=VERIFY,
        n_workers=2,
        queue="coffea-hq-ship-smoke",
        poll_interval=1.0,
        pickle_modules=(shipmod,),
        status=False,
    )
    got, unused = executor(ITEMS, work_fn, {})
    assert unused == 0
    assert got == expected, f"mismatch: {got=} {expected=}"
    print("ok:", got)
