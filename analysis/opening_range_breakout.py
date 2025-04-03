#opening_range_breakout.py

import os
import logging
import datetime
import numpy as np
import pandas as pd
import yfinance as yf
import pytz

# ---------------------------
# Logging Configuration
# ---------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(ch)

# ---------------------------
# Helper: Detect if a given date is in US/Eastern DST
# ---------------------------
def is_us_eastern_dst(date_val):
    """
    Given date_val as a datetime.date, returns True if that date is in DST in US/Eastern.
    Uses midnight UTC -> local conversion to check.
    """
    # Create midnight UTC of that date
    dt_utc = datetime.datetime(date_val.year, date_val.month, date_val.day, tzinfo=pytz.UTC)
    # Convert to US/Eastern
    dt_eastern = dt_utc.astimezone(pytz.timezone("US/Eastern"))
    # Check if it is in DST
    return bool(dt_eastern.dst())

# ---------------------------
# Data Fetching in UTC (no local filtering)
# ---------------------------
def fetch_intraday_data(ticker, days=30, interval="5m"):
    """
    Fetch raw intraday data in UTC, without filtering by time.
    We'll filter for market hours later inside the backtest,
    automatically handling DST on a per‐date basis.
    """
    try:
        df = yf.download(
            ticker,
            period=f"{days}d",
            interval=interval,
            auto_adjust=True,
            progress=False
        )
        if df.empty:
            logger.warning(f"Empty dataset fetched for {ticker}.")
            return pd.DataFrame()
        logger.info(f"Fetched data for {ticker} successfully.")
    except Exception as e:
        logger.error(f"Error fetching data for {ticker}: {e}")
        return pd.DataFrame()

    # Convert MultiIndex columns if necessary
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # Ensure we have a DatetimeIndex in UTC
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)

    # Localize to UTC if naive
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")

    # Optional: store a separate UTC timestamp string for clarity
    df["timestamp_utc"] = df.index.strftime("%Y-%m-%dT%H:%M:%SZ")

    return df

# ---------------------------
# Performance Metrics Calculation
# ---------------------------
def compute_metrics(trades):
    pnl_series = pd.Series(trades['pnl'])
    wins = pnl_series[pnl_series > 0]
    losses = pnl_series[pnl_series <= 0]
    win_rate = len(wins) / len(pnl_series) if len(pnl_series) > 0 else 0.0
    total_win = wins.sum()
    total_loss = abs(losses.sum())
    profit_factor = total_win / total_loss if total_loss != 0 else np.nan

    mean_return = pnl_series.mean()
    std_return = pnl_series.std()
    risk_free_rate = 0.0
    sharpe_ratio = (mean_return - risk_free_rate) / std_return if std_return != 0 else np.nan

    equity_curve = pnl_series.cumsum()
    rolling_max = equity_curve.cummax()
    max_drawdown = (equity_curve - rolling_max).min()

    return {
        "win_rate": round(win_rate, 3),
        "profit_factor": round(profit_factor, 3) if profit_factor == profit_factor else None,
        "sharpe_ratio": round(sharpe_ratio, 3) if sharpe_ratio == sharpe_ratio else None,
        "max_drawdown": round(max_drawdown, 2) if max_drawdown == max_drawdown else None,
        "num_trades": len(trades)
    }

# ---------------------------
# Backtesting Opening Range Breakout with DST Handling (UTC-based)
# ---------------------------
def backtest_opening_range_breakout(
    df,
    open_range_minutes=30,
    use_volume_filter=False,
    stop_loss=None,
    time_exit_minutes=None
):
    """
    Backtest logic in UTC. For each date, detect DST vs. Standard time.
    Then choose the correct "full session" hours and "entry window" hours
    in UTC accordingly.
    """
    if df.empty:
        logger.warning("Empty DataFrame received for backtesting.")
        return pd.DataFrame()

    # Copy data and add a separate 'date_utc' column representing just the UTC date as string
    df = df.copy()
    df["date_utc"] = df.index.strftime("%Y-%m-%d")

    grouped = df.groupby("date_utc")
    all_trades = []

    for day_str, day_data in grouped:
        day_data = day_data.sort_index()
        # Convert day_str back to a date object to check DST, etc.
        date_val = datetime.datetime.strptime(day_str, "%Y-%m-%d").date()

        # Is this date in DST? => pick the correct UTC session times
        if is_us_eastern_dst(date_val):
            # DST: 9:30-16:00 ET -> 13:30-20:00 UTC
            session_start_utc = "13:30"
            session_end_utc   = "20:00"
            # If you only allow entries up to 12:00 ET => 16:00 UTC
            entry_window_start_utc = "13:30"
            entry_window_end_utc   = "16:00"
        else:
            # Standard Time: 9:30-16:00 ET -> 14:30-21:00 UTC
            session_start_utc = "14:30"
            session_end_utc   = "21:00"
            # If you only allow entries up to 12:00 ET => 17:00 UTC
            entry_window_start_utc = "14:30"
            entry_window_end_utc   = "17:00"

        # Filter the "full" day in UTC
        full_day_data = day_data.between_time(session_start_utc, session_end_utc)
        if full_day_data.empty:
            logger.info(f"Skipping {day_str}: No data in session window {session_start_utc}-{session_end_utc} UTC.")
            continue

        # Filter the "entry" data
        entry_data = full_day_data.between_time(entry_window_start_utc, entry_window_end_utc)
        if entry_data.empty:
            logger.info(f"Skipping {day_str}: No data in entry window {entry_window_start_utc}-{entry_window_end_utc}.")
            continue

        # Determine how many bars we need to form the "opening range"
        try:
            interval_minutes = (entry_data.index[1] - entry_data.index[0]).seconds / 60.0
        except Exception as e:
            logger.error(f"Error determining interval for {day_str}: {e}")
            continue

        bars_for_open_range = max(1, int(open_range_minutes // interval_minutes))
        if bars_for_open_range > len(entry_data):
            logger.info(
                f"Skipping {day_str}: insufficient bars for open range, needed {bars_for_open_range}, "
                f"available {len(entry_data)}."
            )
            continue

        # Opening range
        opening_range_data = entry_data.iloc[:bars_for_open_range]
        or_high = opening_range_data["High"].max()
        or_low = opening_range_data["Low"].min()
        or_volume = opening_range_data["Volume"].sum()
        day_avg_volume = entry_data["Volume"].mean()
        volume_ok = or_volume > day_avg_volume

        trade_executed = False
        entry_price = None
        exit_price = None
        direction = None
        trade_entry_time = None
        trade_exit_time = None

        # Loop over the full session to see if/when the breakout occurs
        for idx, row in full_day_data.iterrows():
            # Attempt entry up to the end of the entry window
            if not trade_executed and idx.time() <= datetime.time.fromisoformat(entry_window_end_utc):
                if use_volume_filter and not volume_ok:
                    logger.debug(f"{day_str}: Volume filter not met. Skipping trade.")
                    break
                if row["High"] > or_high:
                    entry_price = or_high
                    direction = "long"
                    trade_executed = True
                    trade_entry_time = idx
                elif row["Low"] < or_low:
                    entry_price = or_low
                    direction = "short"
                    trade_executed = True
                    trade_entry_time = idx

            # If we're in a trade, see if we stop out or time exit
            if trade_executed:
                current_close = row["Close"]
                # Check stop loss
                if stop_loss is not None:
                    if direction == "long" and current_close <= entry_price * (1 - stop_loss):
                        exit_price = entry_price * (1 - stop_loss)
                        trade_exit_time = idx
                        break
                    elif direction == "short" and current_close >= entry_price * (1 + stop_loss):
                        exit_price = entry_price * (1 + stop_loss)
                        trade_exit_time = idx
                        break

                # Check time exit
                if time_exit_minutes is not None and trade_entry_time is not None:
                    elapsed_minutes = (idx - trade_entry_time).seconds / 60.0
                    if elapsed_minutes >= time_exit_minutes:
                        exit_price = current_close
                        trade_exit_time = idx
                        break

        # If a trade was entered but never hit a stop or time exit, exit at session close
        if trade_executed and exit_price is None:
            exit_price = full_day_data.iloc[-1]["Close"]
            trade_exit_time = full_day_data.index[-1]

        # Record the trade if it happened
        if trade_executed:
            pnl = (exit_price - entry_price) if direction == "long" else (entry_price - exit_price)
            trade_record = {
                # Keep the day as the UTC date string, to avoid local offsets
                "date": day_str,  
                "direction": direction,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "pnl": pnl,
                # Store the exact times in UTC ISO8601
                "entry_time": trade_entry_time.astimezone(pytz.UTC).isoformat() if trade_entry_time else None,
                "exit_time": trade_exit_time.astimezone(pytz.UTC).isoformat() if trade_exit_time else None,
            }
            all_trades.append(trade_record)

    if all_trades:
        return pd.DataFrame(all_trades)
    else:
        logger.info("No trades executed during backtesting.")
        return pd.DataFrame()

# ---------------------------
# Running Multiple Test Scenarios
# ---------------------------
def run_opening_range_breakout_tests(ticker, days=30, interval="5m"):
    """
    Run various scenarios, each with different parameters, and
    automatically handle DST for each date.
    """
    results = []
    df = fetch_intraday_data(ticker, days=days, interval=interval)
    if df.empty:
        logger.warning(f"No intraday data available for {ticker}.")
        return {"scenarios": results, "intraday_data": []}

    or_minutes_list = [30, 45]
    volume_options = [False, True]
    stop_loss_options = [None, 0.01]
    time_exit_options = [60, 120]

    for or_minutes in or_minutes_list:
        for use_volume in volume_options:
            for stop_loss in stop_loss_options:
                for time_exit in time_exit_options:
                    scenario_name = f"OR {or_minutes}min"
                    filters_list = []
                    if use_volume:
                        filters_list.append("Volume Filter")
                    if stop_loss is not None:
                        filters_list.append("Stop-Loss")
                    if time_exit is not None:
                        filters_list.append(f"Time Exit ({time_exit}min)")
                    if not filters_list:
                        filters_list.append("No Filters")
                    filter_desc = " + ".join(filters_list)

                    trades_df = backtest_opening_range_breakout(
                        df,
                        open_range_minutes=or_minutes,
                        use_volume_filter=use_volume,
                        stop_loss=stop_loss,
                        time_exit_minutes=time_exit
                    )

                    metrics = compute_metrics(trades_df)
                    daily_trades = trades_df.to_dict(orient="records")
                    net_pnl = trades_df["pnl"].sum() if not trades_df.empty else 0

                    scenario_result = {
                        "scenario_name": scenario_name,
                        "filters": filter_desc,
                        "open_range_minutes": or_minutes,
                        "use_volume_filter": use_volume,
                        "stop_loss": stop_loss,
                        "time_exit_minutes": time_exit,
                        "win_rate": metrics["win_rate"],
                        "win_rate_formatted": f"{round(metrics['win_rate'] * 100, 1)}%",
                        "profit_factor": metrics["profit_factor"],
                        "profit_factor_formatted": (
                            f"{metrics['profit_factor']:.2f}" 
                            if metrics["profit_factor"] is not None else "N/A"
                        ),
                        "sharpe_ratio": metrics["sharpe_ratio"],
                        "sharpe_ratio_formatted": (
                            f"{metrics['sharpe_ratio']:.2f}" 
                            if metrics["sharpe_ratio"] is not None else "N/A"
                        ),
                        "max_drawdown": metrics["max_drawdown"],
                        "max_drawdown_formatted": (
                            f"{metrics['max_drawdown']:.2f}" 
                            if metrics["max_drawdown"] is not None else "N/A"
                        ),
                        "num_trades": metrics["num_trades"],
                        "daily_trades": daily_trades,
                        "net_pnl": round(net_pnl, 2),
                        "net_pnl_formatted": f"${round(net_pnl, 2):,}"
                    }
                    results.append(scenario_result)

    # Prepare intraday data in UTC for the frontend
    intraday_df = df.reset_index(names="timestamp")
    # The original index was in UTC, stored as 'timestamp'
    intraday_df.rename(columns={"index": "timestamp"}, inplace=True)

    # Provide an ISO8601 string in UTC for the 'date' field
    intraday_df["date"] = intraday_df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Rename OHLCV columns for consistency
    intraday_df.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume"
        },
        inplace=True
    )

    intraday_data = intraday_df.to_dict(orient="records")

    return {
        "scenarios": results,
        "intraday_data": intraday_data
    }
