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
    dt_utc = datetime.datetime(date_val.year, date_val.month, date_val.day, tzinfo=pytz.UTC)
    dt_eastern = dt_utc.astimezone(pytz.timezone("US/Eastern"))
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

    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")

    df["timestamp_utc"] = df.index.strftime("%Y-%m-%dT%H:%M:%SZ")

    return df

# ---------------------------
# Performance Metrics Calculation
# ---------------------------
def compute_metrics(trades):
    if trades.empty or "pnl" not in trades.columns:
        return {
            "win_rate": 0.0,
            "profit_factor": None,
            "sharpe_ratio": None,
            "max_drawdown": None,
            "num_trades": 0
        }
    
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
# Backtesting Opening Range Breakout (multiple trades allowed)
# with optional "limit_same_direction" after a losing trade
# ---------------------------
def backtest_opening_range_breakout(
    df,
    open_range_minutes=30,
    use_volume_filter=False,
    stop_loss=None,
    time_exit_minutes=None,
    limit_same_direction=False,
    max_entries=1
):
    """
    Multiple trades (re-entries) logic:
      - If a trade loses and 'limit_same_direction' is True, the next trade 
        for that day MUST be the opposite direction.
      - If limit_same_direction=False, we allow any direction for each new breakout.
      - Up to 'max_entries' trades per day.
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
            entry_window_end_utc   = "16:00"  # 12:00 ET
        else:
            session_start_utc = "14:30"  # 9:30 ET
            session_end_utc   = "21:00"  # 16:00 ET
            entry_window_start_utc = "14:30"
            entry_window_end_utc   = "17:00"  # 12:00 ET

        full_day_data = day_data.between_time(session_start_utc, session_end_utc)
        if full_day_data.empty:
            logger.info(f"Skipping {day_str}: No data in session window {session_start_utc}-{session_end_utc} UTC.")
            continue

        entry_data = full_day_data.between_time(entry_window_start_utc, entry_window_end_utc)
        if entry_data.empty:
            logger.info(f"Skipping {day_str}: No data in entry window {entry_window_start_utc}-{entry_window_end_utc}.")
            continue

        # Determine bars for the "opening range"
        try:
            interval_minutes = (entry_data.index[1] - entry_data.index[0]).seconds / 60.0
        except Exception as e:
            logger.error(f"Error determining interval for {day_str}: {e}")
            continue

        bars_for_open_range = max(1, int(open_range_minutes // interval_minutes))
        if bars_for_open_range > len(entry_data):
            logger.info(
                f"Skipping {day_str}: insufficient bars for open range, "
                f"needed {bars_for_open_range}, available {len(entry_data)}."
            )
            continue

        # Opening range
        opening_range_data = entry_data.iloc[:bars_for_open_range]
        or_high = opening_range_data["High"].max()
        or_low = opening_range_data["Low"].min()
        or_volume = opening_range_data["Volume"].sum()
        day_avg_volume = entry_data["Volume"].mean()
        volume_ok = or_volume > day_avg_volume

        if use_volume_filter and not volume_ok:
            logger.debug(f"{day_str}: Volume filter not met. Skipping trades for the day.")
            continue

        # We'll allow multiple trades, up to max_entries
        trade_count = 0

        # Track state for re-entry constraints
        last_trade_direction = None
        last_trade_was_loss = False

        # Current open trade state
        trade_executed = False
        direction = None
        entry_price = None
        exit_price = None
        trade_entry_time = None
        trade_exit_time = None

        day_trades = []

        for idx, row in full_day_data.iterrows():
            current_time = idx.time()

            # Try to open a new trade if none is open
            if not trade_executed:
                # Allowed to trade more?
                if trade_count < max_entries:
                    # Must also be within entry window
                    if current_time <= datetime.time.fromisoformat(entry_window_end_utc):
                        # Check breakout in both directions
                        go_long = (row["High"] > or_high)
                        go_short = (row["Low"] < or_low)

                        # If the previous trade was a loser, and limit_same_direction=True,
                        # we disallow the same direction as last time.
                        if limit_same_direction and last_trade_was_loss and last_trade_direction is not None:
                            if last_trade_direction == "long":
                                # If last trade was losing long, skip new long
                                go_long = go_long and False
                            else:
                                # If last trade was losing short, skip new short
                                go_short = go_short and False

                        if go_long:
                            direction = "long"
                            entry_price = row['High']
                            trade_entry_time = idx
                            trade_executed = True
                        elif go_short:
                            direction = "short"
                            entry_price = row['Low']
                            trade_entry_time = idx
                            trade_executed = True
                    # else: outside entry window, do nothing
            else:
                # We have a trade open
                if idx == trade_entry_time:
                    # Skip the bar where we just entered
                    continue

                current_close = row["Close"]
                if stop_loss is not None:
                    if direction == "long":
                        stop_price = entry_price * (1 - stop_loss)
                        if row["Low"] <= stop_price:
                            exit_price = stop_price
                            trade_exit_time = idx
                    else:  # short
                        stop_price = entry_price * (1 + stop_loss)
                        if row["High"] >= stop_price:
                            exit_price = stop_price
                            trade_exit_time = idx

                # Time exit
                if exit_price is None and time_exit_minutes is not None and trade_entry_time is not None:
                    elapsed_minutes = (idx - trade_entry_time).seconds / 60.0
                    if elapsed_minutes >= time_exit_minutes:
                        exit_price = current_close
                        trade_exit_time = idx

                # Session close exit if we're at last bar
                is_last_bar = (idx == full_day_data.index[-1])
                if exit_price is None and is_last_bar:
                    exit_price = current_close
                    trade_exit_time = idx

                # Record the trade if we have an exit
                if exit_price is not None:
                    pnl = (exit_price - entry_price) if direction == "long" else (entry_price - exit_price)
                    day_trades.append({
                        "date": day_str,
                        "direction": direction,
                        "entry_price": entry_price,
                        "exit_price": exit_price,
                        "pnl": pnl,
                        "entry_time": trade_entry_time.astimezone(pytz.UTC).isoformat(),
                        "exit_time": trade_exit_time.astimezone(pytz.UTC).isoformat(),
                    })
                    trade_count += 1
                    # Update re-entry logic trackers
                    last_trade_direction = direction
                    last_trade_was_loss = (pnl < 0)

                    # Reset
                    trade_executed = False
                    direction = None
                    entry_price = None
                    exit_price = None
                    trade_entry_time = None
                    trade_exit_time = None

                    if trade_count >= max_entries:
                        # Hit max trades, done for the day
                        break

        # If we somehow end the loop with an open trade, close on final bar
        if trade_executed and entry_price is not None:
            final_close = full_day_data.iloc[-1]["Close"]
            trade_exit_time = full_day_data.index[-1]
            pnl = (final_close - entry_price) if direction == "long" else (entry_price - final_close)
            day_trades.append({
                "date": day_str,
                "direction": direction,
                "entry_price": entry_price,
                "exit_price": final_close,
                "pnl": pnl,
                "entry_time": trade_entry_time.astimezone(pytz.UTC).isoformat(),
                "exit_time": trade_exit_time.astimezone(pytz.UTC).isoformat(),
            })

        if day_trades:
            all_trades.extend(day_trades)

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
    Run various scenarios, each with different parameters, 
    and automatically handle DST for each date.

    Specifically compares:
      - limiting_same_direction=True (force next trade opposite if last was a loser)
      - limiting_same_direction=False (no direction restriction)
    """
    results = []
    df = fetch_intraday_data(ticker, days=days, interval=interval)
    if df.empty:
        logger.warning(f"No intraday data available for {ticker}.")
        return {"scenarios": results, "intraday_data": []}

    or_minutes_list = [30, 45]
    volume_options = [False, True]
    stop_loss_options = [None, 0.005, 0.0075, 0.01]
    time_exit_options = [60, 120]
    limit_same_direction_options = [False, True]
    max_entry_options = [1, 2, 5]

    for or_minutes in or_minutes_list:
        for use_volume in volume_options:
            for stop_loss in stop_loss_options:
                for time_exit in time_exit_options:
                    for limit_dir in limit_same_direction_options:
                        for max_entries in max_entry_options:
                            scenario_name = f"OR {or_minutes}min"
                            filters_list = []
                            if use_volume:
                                filters_list.append("Volume Filter")
                            if stop_loss is not None:
                                filters_list.append(f"Stop-Loss={stop_loss}")
                            if time_exit is not None:
                                filters_list.append(f"Time Exit ({time_exit}min)")
                            if limit_dir:
                                filters_list.append("Force Opposite After Loss")
                            filters_list.append(f"MaxEntries={max_entries}")
                            if not filters_list:
                                filters_list.append("No Filters")

                            filter_desc = " + ".join(filters_list)

                            trades_df = backtest_opening_range_breakout(
                                df,
                                open_range_minutes=or_minutes,
                                use_volume_filter=use_volume,
                                stop_loss=stop_loss,
                                time_exit_minutes=time_exit,
                                limit_same_direction=limit_dir,
                                max_entries=max_entries
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
                                "limit_same_direction": limit_dir,
                                "max_entries": max_entries,
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
    intraday_df.rename(columns={"index": "timestamp"}, inplace=True)

    intraday_df["date"] = intraday_df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
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
