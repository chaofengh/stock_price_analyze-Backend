"""
ORB & Reverse‑ORB engine – now uses *pre‑computed* features
and takes a pre‑calculated `or_levels` lookup to avoid
re‑scanning the first N bars on every run.
"""
import datetime, numpy as np, pandas as pd, pytz
from .date_utils import is_us_eastern_dst
from .logging_config import logger

# ───────── feature builders (called ONCE per request) ─────────
def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    prev_close = df["Close"].shift(1)
    tr = pd.concat(
        [df["High"] - df["Low"],
         (df["High"] - prev_close).abs(),
         (df["Low"]  - prev_close).abs()], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=period).mean()

def compute_intraday_vwap(df: pd.DataFrame) -> pd.Series:
    px_vol = df["Close"] * df["Volume"]
    cum_px_vol = px_vol.groupby(df.index.date).cumsum()
    cum_vol    = df["Volume"].groupby(df.index.date).cumsum()
    return cum_px_vol / cum_vol

# ───────── generic ORB logic ─────────
def _generic_orb_logic(
    df: pd.DataFrame,
    or_levels: dict,                 # 〈── new
    *,
    open_range_minutes: int,
    use_volume_filter: bool,
    use_vwap_filter: bool,
    stop_loss: float | None,
    atr_stop_multiplier: float | None,
    time_exit_minutes: int | None,
    limit_same_direction: bool,
    max_entries: int,
    reverse: bool = False,
):
    if df.empty:
        return pd.DataFrame()

    # columns are already there – they were built once in runner.py
    all_trades = []

    for day_str, day_df in df.groupby("date_utc"):
        session = day_df
        if session.empty:
            continue

        or_high, or_low = or_levels[open_range_minutes][day_str]

        # volume filter
        if use_volume_filter and session.iloc[:open_range_minutes // 5]["Volume"].sum() <= session["Volume"].mean():
            continue

        trade_open = False
        trade_count, last_loss = 0, False
        last_dir, entry_price, entry_time, atr_at_entry, direction = None, None, None, None, None

        for row in session.itertuples(): # slightly faster than .iterrows()
            idx = row.Index   
            if not trade_open:
                if trade_count >= max_entries:
                    continue

                cross_above = row.High > or_high
                cross_below = row.Low  < or_low

                # VWAP bias
                if use_vwap_filter:
                    if cross_above and row.Close < row.vwap: cross_above = False
                    if cross_below and row.Close > row.vwap: cross_below = False

                # after‑loss filter
                if limit_same_direction and last_loss:
                    if last_dir == "long":  cross_above = False
                    if last_dir == "short": cross_below = False

                if not reverse:
                    direction = "long"  if cross_above else "short" if cross_below else None
                else:
                    direction = "short" if cross_above else "long"  if cross_below else None

                if direction:
                    entry_price  = row.High if direction == "long" else row.Low
                    atr_at_entry = row.atr
                    entry_time   = idx
                    trade_open   = True
                    continue

            # -------- manage open position --------
            if trade_open:
                exit_price = None
                if stop_loss is not None:                # pct stop
                    trigger = entry_price * (1 - stop_loss) if direction == "long" \
                              else entry_price * (1 + stop_loss)
                    if (direction == "long"  and row.Low  <= trigger) or \
                       (direction == "short" and row.High >= trigger):
                        exit_price = trigger

                if exit_price is None and atr_stop_multiplier is not None:
                    trigger = entry_price - atr_stop_multiplier * atr_at_entry if direction == "long" \
                              else entry_price + atr_stop_multiplier * atr_at_entry
                    if (direction == "long"  and row.Low  <= trigger) or \
                       (direction == "short" and row.High >= trigger):
                        exit_price = trigger

                if exit_price is None and time_exit_minutes is not None:
                    if (idx - entry_time).seconds // 60 >= time_exit_minutes:
                        exit_price = row.Close

                # end‑of‑session exit
                if exit_price is None and idx == session.index[-1]:
                    exit_price = row.Close

                if exit_price is not None:
                    pnl = exit_price - entry_price if direction == "long" else entry_price - exit_price
                    all_trades.append(
                        {
                            "date": day_str,
                            "direction": direction,
                            "entry_price": round(entry_price, 4),
                            "exit_price":  round(exit_price, 4),
                            "pnl": round(pnl, 4),
                            "entry_time": entry_time.isoformat(),
                            "exit_time":  idx.isoformat(),
                        }
                    )
                    trade_open = False
                    trade_count += 1
                    last_dir, last_loss = direction, pnl < 0

    return pd.DataFrame(all_trades)

# ───────── wrappers ─────────
def backtest_orb(df, or_levels, **kwargs):
    return _generic_orb_logic(df, or_levels, reverse=False, **kwargs)

def backtest_reverse_orb(df, or_levels, **kwargs):
    return _generic_orb_logic(df, or_levels, reverse=True, **kwargs)
