# --------------- bb_sr_strategies.py ---------------
"""
Standalone strategies that use the new feature columns:
    • Bollinger mean‑reversion
    • Support / Resistance breakout‑reversal
Each function MUST accept **kwargs so grid_runner can pass unused params.
"""
import numpy as np
import pandas as pd


# ────────────────────────────────────────────
# 1. Bollinger mean‑reversion (touch‑to‑midline)
# ────────────────────────────────────────────
def backtest_bbands(df: pd.DataFrame, **kwargs):
    trades = []
    in_trade = False
    for row in df.itertuples():
        if not in_trade:
            if row.close < row.BB_lower:
                entry = ("long", row.Index, row.close)
                in_trade = True
            elif row.close > row.BB_upper:
                entry = ("short", row.Index, row.close)
                in_trade = True
            continue

        direction, entry_time, entry_px = entry
        mid = row.BB_middle

        hit_target = (direction == "long"  and row.close >= mid) or \
                     (direction == "short" and row.close <= mid)
        last_bar   = row.Index == df.index[-1]

        if hit_target or last_bar:
            pnl = row.close - entry_px if direction == "long" else entry_px - row.close
            trades.append(
                {"date": entry_time.date().isoformat(),
                 "direction": direction,
                 "entry_price": round(entry_px, 4),
                 "exit_price": round(row.close, 4),
                 "pnl": round(pnl, 4),
                 "entry_time": entry_time.isoformat(),
                 "exit_time": row.Index.isoformat()}
            )
            in_trade = False
    return pd.DataFrame(trades)


# ────────────────────────────────────────────
# 2. S/R breakout‑reversal
# ────────────────────────────────────────────
def backtest_support_resistance(df: pd.DataFrame, **kwargs):
    trades = []
    trade_open = False
    for row in df.itertuples():
        if not trade_open:
            if pd.notna(row.resistance) and row.close > row.resistance:
                entry = ("long", row.Index, row.close, row.support)
                trade_open = True
            elif pd.notna(row.support) and row.close < row.support:
                entry = ("short", row.Index, row.close, row.resistance)
                trade_open = True
            continue

        direction, entry_time, entry_px, stop_lvl = entry
        hit_stop = (direction == "long"  and row.close < stop_lvl) or \
                   (direction == "short" and row.close > stop_lvl)
        last_bar = row.Index == df.index[-1]

        if hit_stop or last_bar:
            pnl = row.close - entry_px if direction == "long" else entry_px - row.close
            trades.append(
                {"date": entry_time.date().isoformat(),
                 "direction": direction,
                 "entry_price": round(entry_px, 4),
                 "exit_price": round(row.close, 4),
                 "pnl": round(pnl, 4),
                 "entry_time": entry_time.isoformat(),
                 "exit_time": row.Index.isoformat()}
            )
            trade_open = False
    return pd.DataFrame(trades)
