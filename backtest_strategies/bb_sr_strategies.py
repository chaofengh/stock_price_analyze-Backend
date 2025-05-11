# --------------- bb_sr_strategies.py ---------------
"""
Standalone strategies that use the new feature columns:
    • Bollinger mean‑reversion
    • Support / Resistance breakout‑reversal
Each function MUST accept **kwargs so grid_runner can pass unused params.
"""
import numpy as np
import pandas as pd

 # ─────────────────────────────────────────────────────────
# 1. Bollinger mean‑reversion (lower‑to‑upper & vice versa)
# ─────────────────────────────────────────────────────────
def backtest_bbands(
    df: pd.DataFrame,
    *,
    max_entries: int = 1,
    **kwargs,
) -> pd.DataFrame:
    """
    Long when price closes below the LOWER band,
    exit when it touches the UPPER band (or last bar).

    Short when price closes above the UPPER band,
    exit when it touches the LOWER band (or last bar).

    Limits to `max_entries` trades **per trading day**.
    """
    trades = []

    # Group one session (= one calendar day) at a time
    for day, session in df.groupby(df.index.date, sort=False):
        trade_open = False
        trade_count = 0

        for i, row in enumerate(session.itertuples()):
            last_bar = i == len(session) - 1

            # ───── ENTRY ─────
            if not trade_open and trade_count < max_entries:
                if row.close <= row.BB_lower or row.open <= row.BB_lower:                 # LONG entry
                    direction = "long"
                    entry_px  = row.close
                    entry_t   = row.Index
                    trade_open = True
                    continue

                if row.close >= row.BB_upper or row.open >= row.BB_upper:                 # SHORT entry
                    direction = "short"
                    entry_px  = row.close
                    entry_t   = row.Index
                    trade_open = True
                    continue

            # ───── EXIT ─────
            if trade_open:
                exit_now = False

                # Touch opposite band
                if (direction == "long"  and (row.close >= row.BB_upper or row.open >= row.BB_upper)) or \
                   (direction == "short" and (row.close  <= row.BB_lower or row.open <= row.BB_lower)):
                    exit_now = True

                # Or flush on last bar of the day
                if last_bar and not exit_now:
                    exit_now = True

                if exit_now:
                    exit_px = row.close
                    pnl = exit_px - entry_px if direction == "long" else entry_px - exit_px

                    trades.append({
                        "date":        entry_t.date().isoformat(),
                        "direction":   direction,
                        "entry_price": round(entry_px, 4),
                        "exit_price":  round(exit_px, 4),
                        "pnl":         round(pnl, 4),
                        "entry_time":  entry_t.isoformat(),
                        "exit_time":   row.Index.isoformat(),
                    })
                    trade_open  = False
                    trade_count += 1

    return pd.DataFrame(trades)


# ───────────────────────────────────────────
# 2. Support / Resistance breakout‑reversal
# ───────────────────────────────────────────
def backtest_support_resistance(
    df: pd.DataFrame,
    *,
    max_entries: int = 1,
    **kwargs,
) -> pd.DataFrame:
    """
    Long when close pierces *above* resistance, stop if it falls below paired support.
    Short when close pierces *below* support, stop if it rises above paired resistance.

    Obeys `max_entries` trades per day.
    """
    trades = []

    for day, session in df.groupby(df.index.date, sort=False):
        trade_open = False
        trade_count = 0

        for i, row in enumerate(session.itertuples()):
            last_bar = i == len(session) - 1

            # ───── ENTRY ─────
            if not trade_open and trade_count < max_entries:
                if pd.notna(row.resistance) and row.close > row.resistance:
                    direction = "long"
                    entry_px  = row.close
                    stop_lvl  = row.support    # potential flip line
                    entry_t   = row.Index
                    trade_open = True
                    continue

                if pd.notna(row.support) and row.close < row.support:
                    direction = "short"
                    entry_px  = row.close
                    stop_lvl  = row.resistance
                    entry_t   = row.Index
                    trade_open = True
                    continue

            # ───── EXIT ─────
            if trade_open:
                exit_now = False

                # Flip back through the stop level
                if (direction == "long"  and row.close < stop_lvl) or \
                   (direction == "short" and row.close > stop_lvl):
                    exit_now = True

                # Or flush on last bar
                if last_bar and not exit_now:
                    exit_now = True

                if exit_now:
                    exit_px = row.close
                    pnl = exit_px - entry_px if direction == "long" else entry_px - exit_px

                    trades.append({
                        "date":        entry_t.date().isoformat(),
                        "direction":   direction,
                        "entry_price": round(entry_px, 4),
                        "exit_price":  round(exit_px, 4),
                        "pnl":         round(pnl, 4),
                        "entry_time":  entry_t.isoformat(),
                        "exit_time":   row.Index.isoformat(),
                    })
                    trade_open  = False
                    trade_count += 1

    return pd.DataFrame(trades)
