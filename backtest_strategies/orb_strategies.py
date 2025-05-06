# ORB & Reverse‑ORB engine – mutually‑exclusive exit styles
from __future__ import annotations
import numpy as np
import pandas as pd
from typing import Optional
from .logging_config import logger


# ───── helper indicators ───────────────────────────────────────
def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [df["high"] - df["low"],
         (df["high"] - prev_close).abs(),
         (df["low"]  - prev_close).abs()], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=period).mean()


def compute_intraday_vwap(df: pd.DataFrame) -> pd.Series:
    pv       = df["close"] * df["volume"]
    cum_pv   = pv.groupby(df.index.date).cumsum()
    cum_vol  = df["volume"].groupby(df.index.date).cumsum()
    return cum_pv / cum_vol


# ───── core back‑test loop ────────────────────────────────────
def _generic_orb_logic(
    df: pd.DataFrame,
    or_levels: dict,
    *,
    open_range_minutes: int,
    use_volume_filter: bool,
    use_vwap_filter: bool,
    stop_loss: Optional[float],
    atr_stop_multiplier: Optional[float],
    time_exit_minutes: Optional[int],
    use_bb_exit: bool,
    use_sr_exit: bool,
    limit_same_direction: bool,
    max_entries: int,
    reverse: bool = False,
) -> pd.DataFrame:
    """
    Pure‑Python ORB / Reverse‑ORB back‑tester.
    Uses **row.close** for break‑out detection but *waits*
    until the open‑range block has fully printed.
    """
    if df.empty:
        return pd.DataFrame()

    trades = []

    for day_str, session in df.groupby("date_utc"):

        # safety — malformed sessions
        if len(session) < 2 or day_str not in or_levels[open_range_minutes]:
            continue

        # granular timing info
        bar_min  = int((session.index[1] - session.index[0]).seconds / 60)
        or_bars  = max(1, open_range_minutes // bar_min)
        or_high, or_low = or_levels[open_range_minutes][day_str]

        # guard against NaN OR values
        if pd.isna(or_high) or pd.isna(or_low):
            continue

        # optional volume filter ON THE OR BLOCK
        if use_volume_filter:
            vol_block = session.iloc[:or_bars]["volume"].sum()
            if vol_block <= session["volume"].mean():
                continue

        first_tradable_bar = (
            session.index[or_bars] if len(session) > or_bars else None
        )

        trade_open, trade_count = False, 0
        last_loss, last_dir     = False, None

        for row in session.itertuples():
            idx = row.Index

            # ───────────────── ENTRY ─────────────────
            if not trade_open and trade_count < max_entries:

                # wait until OR window is over
                if first_tradable_bar and idx < first_tradable_bar:
                    continue

                cross_above = row.close > or_high
                cross_below = row.close < or_low

                # VWAP filter (intraday trend confirmation)
                if use_vwap_filter:
                    if cross_above and row.close < row.vwap:
                        cross_above = False
                    if cross_below and row.close > row.vwap:
                        cross_below = False

                # “don’t repeat same‑direction after loss”
                if limit_same_direction and last_loss:
                    if last_dir == "long":
                        cross_above = False
                    if last_dir == "short":
                        cross_below = False

                direction = (
                    ("short" if reverse else "long")  if cross_above else
                    ("long"  if reverse else "short") if cross_below else
                    None
                )

                if direction:
                    entry_px     = row.close
                    atr_at_entry = row.atr
                    entry_time   = idx
                    trade_open   = True
                    continue

            # ───────────────── MANAGEMENT ────────────────
            if trade_open:
                exit_px = None

                # 1) Bollinger profit‑take
                if use_bb_exit:
                    if (direction == "short" and row.low  <= row.BB_lower) or \
                       (direction == "long"  and row.high >= row.BB_upper):
                        exit_px = row.close

                # 2) S/R flip
                if exit_px is None and use_sr_exit:
                    if direction == "long"  and pd.notna(row.support)    and row.close < row.support:
                        exit_px = row.close
                    if direction == "short" and pd.notna(row.resistance) and row.close > row.resistance:
                        exit_px = row.close

                # 3) %-stop
                if exit_px is None and stop_loss is not None:
                    trg = entry_px * (1 - stop_loss) if direction == "long" else entry_px * (1 + stop_loss)
                    if (direction == "long" and row.low <= trg) or \
                       (direction == "short" and row.high >= trg):
                        exit_px = trg

                # 4) ATR stop
                if exit_px is None and atr_stop_multiplier is not None:
                    trg = (
                        entry_px - atr_stop_multiplier * atr_at_entry if direction == "long"
                        else entry_px + atr_stop_multiplier * atr_at_entry
                    )
                    if (direction == "long" and row.low <= trg) or \
                       (direction == "short" and row.high >= trg):
                        exit_px = trg

                # 5) time‑based exit
                if (
                    exit_px is None and time_exit_minutes is not None and
                    (idx - entry_time).total_seconds() // 60 >= time_exit_minutes
                ):
                    exit_px = row.close

                # 6) end‑of‑session flush
                if exit_px is None and idx == session.index[-1]:
                    exit_px = row.close

                # ───── record trade ─────
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


# ───── wrappers ────────────────────────────────────────────────
def backtest_orb(df, or_levels, **kw):
    return _generic_orb_logic(df, or_levels, reverse=False, **kw)


def backtest_reverse_orb(df, or_levels, **kw):
    return _generic_orb_logic(df, or_levels, reverse=True, **kw)
