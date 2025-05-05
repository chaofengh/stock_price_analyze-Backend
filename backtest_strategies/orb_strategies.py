# backtest_strategies/orb_strategies.py
# =====================================
"""
ORB & Reverse‑ORB back‑tester

* Adds an optional Numba JIT wrapper – disabled by default because
  DataFrames/dicts aren’t numba‑friendly.  When you refactor the
  inner loop to plain NumPy arrays, flip the flag to `True` (or
  add your own type check) and you’ll get a 2‑3× speed boost.

* Contains the S/R‑aware exit logic:
    – S/R flip‑exit is **disabled** whenever `time_exit_minutes`
      is not `None` (e.g., 60‑minute timed exit runs).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Optional
from .logging_config import logger

# ───────── optional numba – safe fallback ─────────
try:
    from numba import njit
    _NUMBA_AVAILABLE = True
except ImportError:
    _NUMBA_AVAILABLE = False


# ────────────────────────────────────────────────────
# Helper feature builders (unchanged)
# ────────────────────────────────────────────────────
def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [df["high"] - df["low"],
         (df["high"] - prev_close).abs(),
         (df["low"]  - prev_close).abs()], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=period).mean()


def compute_intraday_vwap(df: pd.DataFrame) -> pd.Series:
    pv   = df["close"] * df["volume"]
    cum_pv  = pv.groupby(df.index.date).cumsum()
    cum_vol = df["volume"].groupby(df.index.date).cumsum()
    return cum_pv / cum_vol


# ────────────────────────────────────────────────────
# Core ORB engine (pure‑Python)
# ────────────────────────────────────────────────────
def _generic_orb_logic_py(
    df: pd.DataFrame,
    or_levels: dict,
    *,
    open_range_minutes: int,
    use_volume_filter: bool,
    use_vwap_filter: bool,
    stop_loss: Optional[float],
    atr_stop_multiplier: Optional[float],
    time_exit_minutes: Optional[int],
    limit_same_direction: bool,
    max_entries: int,
    reverse: bool = False,
) -> pd.DataFrame:
    """Return a trade list DataFrame for one strategy/parameter set."""
    if df.empty:
        return pd.DataFrame()

    trades = []

    for day_str, session in df.groupby("date_utc"):
        if session.empty:
            continue

        or_high, or_low = or_levels[open_range_minutes][day_str]

        # ───── volume filter on the OR block ─────
        if use_volume_filter:
            vol_block = session.iloc[: open_range_minutes // 5]["volume"].sum()
            if vol_block <= session["volume"].mean():
                continue

        trade_open = False
        trade_count, last_loss = 0, False
        last_dir = None

        for row in session.itertuples():
            idx = row.Index

            # ────────────────────── ENTRY ──────────────────────
            if not trade_open and trade_count < max_entries:
                cross_above = row.close > or_high
                cross_below = row.close < or_low

                # vwap confirmation
                if use_vwap_filter:
                    if cross_above and row.close < row.vwap:
                        cross_above = False
                    if cross_below and row.close > row.vwap:
                        cross_below = False

                # limit same‑direction after a loss
                if limit_same_direction and last_loss:
                    if last_dir == "long":
                        cross_above = False
                    if last_dir == "short":
                        cross_below = False

                direction = (
                    ("short" if reverse else "long") if cross_above else
                    ("long"  if reverse else "short") if cross_below else None
                )

                if direction:
                    entry_px     = row.close
                    atr_at_entry = row.atr
                    entry_time   = idx
                    trade_open   = True
                    continue  # proceed to next candle while in trade

            # ────────────────────── MANAGE OPEN POSITION ──────────────────────
            if trade_open:
                exit_px = None

                # 1) Bollinger profit‑take
                if (direction == "short" and row.low  <= row.BB_lower) or \
                   (direction == "long"  and row.high >= row.BB_upper):
                    exit_px = row.close

                # 2) S/R flip – enabled only if NO timed exit
                if (
                    exit_px is None and
                    time_exit_minutes is None and
                    pd.notna(row.support) and pd.notna(row.resistance)
                ):
                    if (direction == "short" and row.close > row.resistance) or \
                       (direction == "long"  and row.close < row.support):
                        exit_px = row.close

                # 3) %-stop
                if exit_px is None and stop_loss is not None:
                    trg = entry_px * (1 - stop_loss) if direction == "long" else entry_px * (1 + stop_loss)
                    if (direction == "long" and row.low <= trg) or \
                       (direction == "short" and row.high >= trg):
                        exit_px = trg

                # 4) ATR‑stop
                if exit_px is None and atr_stop_multiplier is not None:
                    trg = (
                        entry_px - atr_stop_multiplier * atr_at_entry if direction == "long"
                        else entry_px + atr_stop_multiplier * atr_at_entry
                    )
                    if (direction == "long" and row.low <= trg) or \
                       (direction == "short" and row.high >= trg):
                        exit_px = trg

                # 5) timed exit
                if (
                    exit_px is None and
                    time_exit_minutes is not None and
                    (idx - entry_time).seconds // 60 >= time_exit_minutes
                ):
                    exit_px = row.close

                # 6) end‑of‑session flush
                if exit_px is None and idx == session.index[-1]:
                    exit_px = row.close

                # ───────── record trade ─────────
                if exit_px is not None:
                    pnl = exit_px - entry_px if direction == "long" else entry_px - exit_px
                    trades.append(
                        {
                            "date": day_str,
                            "direction": direction,
                            "entry_price": round(entry_px, 4),
                            "exit_price": round(exit_px, 4),
                            "pnl": round(pnl, 4),
                            "entry_time": entry_time.isoformat(),
                            "exit_time": idx.isoformat(),
                        }
                    )
                    trade_open   = False
                    trade_count += 1
                    last_dir, last_loss = direction, pnl < 0

    return pd.DataFrame(trades)


# ────────────────────────────────────────────────────
# Pick implementation (Numba later, Python now)
# ────────────────────────────────────────────────────
# Flip the flag below to `True` only after you refactor the
# inner loop to accept plain NumPy arrays instead of DataFrames.
_USE_NUMBA_JIT = False   # ← keep False for stability

if _USE_NUMBA_JIT and _NUMBA_AVAILABLE:
    _generic_orb_logic = njit(cache=True, nogil=True)(_generic_orb_logic_py)
else:
    _generic_orb_logic = _generic_orb_logic_py


# ────────────────────────────────────────────────────
# Public wrappers
# ────────────────────────────────────────────────────
def backtest_orb(df, or_levels, **kw):
    """Normal ORB (breakout in OR direction)."""
    return _generic_orb_logic(df, or_levels, reverse=False, **kw)


def backtest_reverse_orb(df, or_levels, **kw):
    """Reverse ORB (fade the initial breakout)."""
    return _generic_orb_logic(df, or_levels, reverse=True, **kw)
