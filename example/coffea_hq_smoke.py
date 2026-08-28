"""Smoke test for CoffeaHQExecutor with fake items (no ROOT/NanoAOD).

Requires redis + TLS HQ server and:
  export HQ_RESULT_DIR=/tmp/hq-results
"""

from pathlib import Path

from coffea.processor import accumulate

from hq.coffea import CoffeaHQExecutor

HOST = "https://localhost"
PORT = 3000
VERIFY = str(Path(__file__).resolve().parents[1] / "cert.pem")

ITEMS = list(range(10))


def map_fun(i: int) -> dict[str, int]:
    return {"n": i, "count": 1}


if __name__ == "__main__":
    seed: dict[str, int] = {}
    expected = accumulate([map_fun(i) for i in ITEMS], seed)

    executor = CoffeaHQExecutor(
        host=HOST,
        port=PORT,
        verify=VERIFY,
        n_workers=2,
        queue="coffea-hq-smoke",
        poll_interval=1.0,
        status=False,
    )
    got, unused = executor(ITEMS, map_fun, {})
    assert unused == 0
    assert got == expected, f"mismatch: {got=} {expected=}"
    print("ok:", got)
