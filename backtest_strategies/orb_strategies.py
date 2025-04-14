# backtest_strategies/orb_strategies.py

import datetime
import pandas as pd
import pytz
from .date_utils import is_us_eastern_dst
from .logging_config import logger

def _generic_orb_logic(
    df,
    open_range_minutes,
    use_volume_filter,
    stop_loss,
    time_exit_minutes,
    limit_same_direction,
    max_entries,
    reverse=False
):
    """
    The core ORB logic, with a 'reverse' boolean to invert direction.
    If reverse=False, crossing above the OR high => long, else short.
    If reverse=True, crossing above the OR high => short, else long.
    """
    if df.empty:
        logger.warning("Empty DataFrame received for backtesting.")
        return pd.DataFrame()

    df = df.copy()
    df["date_utc"] = df.index.strftime("%Y-%m-%d")
    grouped = df.groupby("date_utc")
    all_trades = []

    for day_str, day_data in grouped:
        day_data = day_data.sort_index()
        date_val = datetime.datetime.strptime(day_str, "%Y-%m-%d").date()

        # Determine session times based on DST
        if is_us_eastern_dst(date_val):
            session_start_utc = "13:30"  # 9:30 ET
            session_end_utc   = "20:00"  # 16:00 ET
            entry_window_start_utc = "13:30"
            entry_window_end_utc   = "16:00"
        else:
            session_start_utc = "14:30"  # 9:30 ET
            session_end_utc   = "21:00"  # 16:00 ET
            entry_window_start_utc = "14:30"
            entry_window_end_utc   = "17:00"

        full_day_data = day_data.between_time(session_start_utc, session_end_utc)
        if full_day_data.empty:
            continue

        entry_data = full_day_data.between_time(entry_window_start_utc, entry_window_end_utc)
        if entry_data.empty:
            continue

        # Determine how many bars define the opening range
        try:
            interval_minutes = (entry_data.index[1] - entry_data.index[0]).seconds / 60.0
        except Exception:
            continue

        bars_for_open_range = max(1, int(open_range_minutes // interval_minutes))
        if bars_for_open_range > len(entry_data):
            continue

        # Opening range
        opening_range_data = entry_data.iloc[:bars_for_open_range]
        or_high = opening_range_data["High"].max()
        or_low = opening_range_data["Low"].min()

        # Volume filter
        or_volume = opening_range_data["Volume"].sum()
        day_avg_volume = entry_data["Volume"].mean()
        volume_ok = or_volume > day_avg_volume
        if use_volume_filter and not volume_ok:
            continue

        trade_count = 0
        last_trade_direction = None
        last_trade_was_loss = False

        trade_open = False
        direction = None
        entry_price = None
        exit_price = None
        trade_entry_time = None
        trade_exit_time = None

        for idx, row in full_day_data.iterrows():
            current_time = idx.time()

            if not trade_open:
                if trade_count < max_entries:
                    if current_time <= datetime.time.fromisoformat(entry_window_end_utc):
                        # Detect breakouts
                        cross_above = (row["High"] > or_high)
                        cross_below = (row["Low"] < or_low)

                        if limit_same_direction and last_trade_was_loss and last_trade_direction:
                            if last_trade_direction == "long":
                                cross_above = False
                            if last_trade_direction == "short":
                                cross_below = False

                        if not reverse:
                            if cross_above:
                                direction = "long"
                                entry_price = row['High']
                                trade_open = True
                                trade_entry_time = idx
                            elif cross_below:
                                direction = "short"
                                entry_price = row['Low']
                                trade_open = True
                                trade_entry_time = idx
                        else:
                            if cross_above:
                                direction = "short"
                                entry_price = row['High']
                                trade_open = True
                                trade_entry_time = idx
                            elif cross_below:
                                direction = "long"
                                entry_price = row['Low']
                                trade_open = True
                                trade_entry_time = idx
            else:
                if idx == trade_entry_time:
                    continue

                current_close = row["Close"]
                if stop_loss is not None:
                    if direction == "long":
                        stop_price = entry_price * (1 - stop_loss)
                        if row["Low"] <= stop_price:
                            exit_price = stop_price
                            trade_exit_time = idx
                    else:
                        stop_price = entry_price * (1 + stop_loss)
                        if row["High"] >= stop_price:
                            exit_price = stop_price
                            trade_exit_time = idx

                if exit_price is None and time_exit_minutes is not None and trade_entry_time:
                    elapsed = (idx - trade_entry_time).seconds / 60.0
                    if elapsed >= time_exit_minutes:
                        exit_price = current_close
                        trade_exit_time = idx

                is_last_bar = (idx == full_day_data.index[-1])
                if exit_price is None and is_last_bar:
                    exit_price = current_close
                    trade_exit_time = idx

                if exit_price is not None:
                    pnl = (exit_price - entry_price) if direction == "long" else (entry_price - exit_price)
                    all_trades.append({
                        "date": day_str,
                        "direction": direction,
                        "entry_price": entry_price,
                        "exit_price": exit_price,
                        "pnl": pnl,
                        "entry_time": trade_entry_time.astimezone(pytz.UTC).isoformat(),
                        "exit_time": trade_exit_time.astimezone(pytz.UTC).isoformat(),
                    })
                    trade_count += 1
                    last_trade_direction = direction
                    last_trade_was_loss = (pnl < 0)
                    trade_open = False
                    direction = None
                    entry_price = None
                    exit_price = None
                    trade_entry_time = None
                    trade_exit_time = None
                    if trade_count >= max_entries:
                        break

        if trade_open and entry_price is not None:
            final_close = full_day_data.iloc[-1]["Close"]
            trade_exit_time = full_day_data.index[-1]
            pnl = (final_close - entry_price) if direction == "long" else (entry_price - final_close)
            all_trades.append({
                "date": day_str,
                "direction": direction,
                "entry_price": entry_price,
                "exit_price": final_close,
                "pnl": pnl,
                "entry_time": trade_entry_time.astimezone(pytz.UTC).isoformat(),
                "exit_time": trade_exit_time.astimezone(pytz.UTC).isoformat(),
            })

    return pd.DataFrame(all_trades) if all_trades else pd.DataFrame()

def backtest_orb(
    df,
    open_range_minutes=30,
    use_volume_filter=False,
    stop_loss=None,
    time_exit_minutes=None,
    limit_same_direction=False,
    max_entries=1
):
    """
    Standard Opening Range Breakout:
      - If price moves above the opening range high, go long.
      - If price moves below the opening range low, go short.
    """
    return _generic_orb_logic(
        df,
        open_range_minutes=open_range_minutes,
        use_volume_filter=use_volume_filter,
        stop_loss=stop_loss,
        time_exit_minutes=time_exit_minutes,
        limit_same_direction=limit_same_direction,
        max_entries=max_entries,
        reverse=False
    )

def backtest_reverse_orb(
    df,
    open_range_minutes=30,
    use_volume_filter=False,
    stop_loss=None,
    time_exit_minutes=None,
    limit_same_direction=False,
    max_entries=1
):
    """
    Reverse Opening Range Breakout:
      - If price moves above the opening range high, go short.
      - If price moves below the opening range low, go long.
    """
    return _generic_orb_logic(
        df,
        open_range_minutes=open_range_minutes,
        use_volume_filter=use_volume_filter,
        stop_loss=stop_loss,
        time_exit_minutes=time_exit_minutes,
        limit_same_direction=limit_same_direction,
        max_entries=max_entries,
        reverse=True
    )
