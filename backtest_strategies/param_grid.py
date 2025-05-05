"""
Central hub for parameter‑sweep definitions *with light pruning*
so we don’t waste CPU on clearly redundant combos.
"""
from itertools import product

OPEN_RANGE_MINUTES   = [5, 10, 15, 30, 45]

STOP_LOSS_PCTS       = [None, 0.003, 0.005, 0.01]
ATR_STOP_MULTIPLIERS = [None, 1.0, 1.5, 2.0]

TIME_EXIT_MINUTES    = [5, 10, 15, 30, 60, 90, 120, 180, 240, None]

USE_VOLUME_FILTER    = [False, True]
USE_VWAP_FILTER      = [False, True]
LIMIT_SAME_DIR       = [False, True]
MAX_ENTRIES          = [1, 2]

def generate():
    keys = (
        "open_range_minutes",
        "use_volume_filter",
        "use_vwap_filter",
        "stop_loss",
        "atr_stop_multiplier",
        "time_exit_minutes",
        "limit_same_direction",
        "max_entries",
    )

    for values in product(
        OPEN_RANGE_MINUTES,
        USE_VOLUME_FILTER,
        USE_VWAP_FILTER,
        STOP_LOSS_PCTS,
        ATR_STOP_MULTIPLIERS,
        TIME_EXIT_MINUTES,
        LIMIT_SAME_DIR,
        MAX_ENTRIES,
    ):
        p = dict(zip(keys, values))

        # ───────────── pruning rules ─────────────
        # 1) can't exit before OR finishes
        if p["time_exit_minutes"] is not None and p["time_exit_minutes"] < p["open_range_minutes"]:
            continue
        # 2) skip combos with no protective stop
        if p["stop_loss"] is None and p["atr_stop_multiplier"] is None:
            continue

        yield p
