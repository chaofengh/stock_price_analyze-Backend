#param_grid.py
"""
Central hub for parameter‑sweep definitions.
"""
from itertools import product

OPEN_RANGE_MINUTES   = [5, 10, 15, 30, 45]

STOP_LOSS_PCTS       = [None, 0.003, 0.005, 0.01]
ATR_STOP_MULTIPLIERS = [None, 1.0, 1.5, 2.0]

TIME_EXIT_MINUTES    = [5, 10, 15, 30, 60, 90, 120,180,240, None]

USE_VOLUME_FILTER    = [False, True]
USE_VWAP_FILTER      = [False, True]      # NEW  ←─────────────
LIMIT_SAME_DIR       = [False, True]
MAX_ENTRIES          = [1, 2]

def generate():
    keys = (
        "open_range_minutes",
        "use_volume_filter",
        "use_vwap_filter",        # NEW
        "stop_loss",
        "atr_stop_multiplier",
        "time_exit_minutes",
        "limit_same_direction",
        "max_entries",
    )

    for values in product(
        OPEN_RANGE_MINUTES,
        USE_VOLUME_FILTER,
        USE_VWAP_FILTER,          # NEW
        STOP_LOSS_PCTS,
        ATR_STOP_MULTIPLIERS,
        TIME_EXIT_MINUTES,
        LIMIT_SAME_DIR,
        MAX_ENTRIES,
    ):
        yield dict(zip(keys, values))
