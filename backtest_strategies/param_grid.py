#param_grid.py

from itertools import product

# ─── core ranges ───────────────────────────────────────────────
OPEN_RANGE_MINUTES = [5, 10, 15, 30, 45]

USE_VOLUME_FILTER  = [False, True]
USE_VWAP_FILTER    = [False, True]
LIMIT_SAME_DIR     = [False, True]
MAX_ENTRIES        = [1, 2]

# ─── exclusive exit‑style knobs ────────────────────────────────
TIME_EXIT_MINUTES    = [5, 10, 15, 30, 60, 90, 120, 180, 240, None]

STOP_LOSS_PCTS       = [0.003, 0.005, 0.01, None]     # None ⇢ off
ATR_STOP_MULTIPLIERS = [1.0, 1.5, 2.0, None]          # None ⇢ off

USE_BB_EXIT          = [False, True]
USE_SR_EXIT          = [False, True]

# ─── grid generator with exclusivity filter ───────────────────
def generate():
    keys = (
        "open_range_minutes",
        "use_volume_filter",
        "use_vwap_filter",
        "stop_loss",
        "atr_stop_multiplier",
        "time_exit_minutes",
        "use_bb_exit",
        "use_sr_exit",
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
        USE_BB_EXIT,
        USE_SR_EXIT,
        LIMIT_SAME_DIR,
        MAX_ENTRIES,
    ):
        p = dict(zip(keys, values))

        # Only ONE exit style may be active
        active = sum([
            p["time_exit_minutes"] is not None,
            p["stop_loss"]         is not None,
            p["atr_stop_multiplier"] is not None,
            p["use_bb_exit"],
            p["use_sr_exit"],
        ])
        if active != 1:
            continue

        yield p
