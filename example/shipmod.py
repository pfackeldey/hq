"""VALUE used by coffea_hq_ship_smoke to prove pickle-by-value shipping."""

SHIP_SCALE = 3


def scale(n: int) -> int:
    return n * SHIP_SCALE
