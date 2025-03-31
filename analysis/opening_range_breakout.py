# analysis/opening_range_breakout.py

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
# Caching Setup
# ---------------------------
CACHE_DIR = "cache"
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

# ---------------------------
# Data Fetching and Preprocessing
# ---------------------------
def fetch_intraday_data(ticker, days=30, interval="15m", target_timezone="US/Eastern", force_refresh=False):
    """
    Fetch intraday data from yfinance with caching.
    - Converts data from UTC to the target timezone.
    - Filters data to regular market hours (9:30 to 16:00 US/Eastern).

    Parameters:
        ticker (str): The stock ticker.
        days (int): Number of calendar days to fetch.
        interval (str): Data interval (e.g., "15m").
        target_timezone (str): Timezone to convert data into.
        force_refresh (bool): If True, ignore cache and fetch new data.

    Returns:
        pd.DataFrame: The intraday data with timezone-adjusted index.
    """
    cache_file = os.path.join(CACHE_DIR, f"{ticker}_{days}d_{interval}.csv")
    df = None

    # Attempt to load cached data
    if not force_refresh and os.path.exists(cache_file):
        try:
            df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            logger.info(f"Loaded cached data for {ticker} from {cache_file}")
        except Exception as e:
            logger.error(f"Failed to load cache for {ticker}: {e}")
            df = None

    # Fetch data if not cached or if force_refresh is True
    if df is None or df.empty:
        try:
            df = yf.download(ticker, period=f"{days}d", interval=interval, auto_adjust=True, progress=False)
            if df.empty:
                logger.warning(f"Empty dataset fetched for {ticker}.")
                return pd.DataFrame()
            df.to_csv(cache_file)
            logger.info(f"Fetched and cached data for {ticker} into {cache_file}")
        except Exception as e:
            logger.error(f"Error fetching data for {ticker}: {e}")
            return pd.DataFrame()

    # Ensure index is timezone-aware (assume UTC if not)
    if df.index.tzinfo is None:
        df.index = df.index.tz_localize("UTC")
    try:
        df.index = df.index.tz_convert(target_timezone)
    except Exception as e:
        logger.error(f"Timezone conversion error for {ticker}: {e}")
        return pd.DataFrame()

    # Filter for regular market hours (using between_time)
    try:
        df = df.between_time("09:30", "16:00")
    except Exception as e:
        logger.error(f"Error filtering market hours for {ticker}: {e}")

    return df

# ---------------------------
# Performance Metrics Calculation
# ---------------------------
def compute_metrics(trades):
    """
    Compute key performance metrics from a DataFrame of trades.
    Metrics include:
      - Win Rate
      - Profit Factor
      - Sharpe Ratio (naively computed)
      - Maximum Drawdown

    Parameters:
        trades (pd.DataFrame): Must include a 'pnl' column.

    Returns:
        dict: Metrics as a dictionary.
    """
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
# Backtesting the Opening Range Breakout Strategy
# ---------------------------
def backtest_opening_range_breakout(
    df,
    open_range_minutes=30,
    use_volume_filter=False,
    stop_loss=None,
    time_exit_minutes=None,
    trade_entry_window=("09:30", "12:00")
):
    """
    Backtest the Opening Range Breakout strategy with improved robustness:
      - Only considers trades within the defined trade entry window.
      - Handles edge cases like missing data or low liquidity.
      - Logs warnings for days with insufficient data.

    Parameters:
        df (pd.DataFrame): Intraday data (must have timezone-aware index).
        open_range_minutes (int): Duration in minutes to define the opening range.
        use_volume_filter (bool): Only trade if opening range volume exceeds daily average.
        stop_loss (float): Percentage (e.g., 0.01 for 1%) stop-loss threshold.
        time_exit_minutes (int): Minutes after entry to force exit.
        trade_entry_window (tuple): (start_time, end_time) for trade entries (e.g., ("09:30", "12:00")).

    Returns:
        pd.DataFrame: Trade records with columns: date, direction, entry_price, exit_price, pnl.
    """
    if df.empty:
        logger.warning("Empty DataFrame received for backtesting.")
        return pd.DataFrame()

    # Group by trading day using a dedicated 'date' column
    df = df.copy()
    df["date"] = df.index.date
    grouped = df.groupby("date")
    all_trades = []

    for date_val, day_data in grouped:
        day_data = day_data.sort_index()

        # Define the trade entry window for the day
        try:
            tz = day_data.index.tz
            entry_start = pd.Timestamp(f"{date_val} {trade_entry_window[0]}").tz_localize(tz)
            entry_end = pd.Timestamp(f"{date_val} {trade_entry_window[1]}").tz_localize(tz)
        except Exception as e:
            logger.error(f"Session time error for {date_val}: {e}")
            continue

        # Filter for the entry window; skip if no data remains
        session_data = day_data.between_time(trade_entry_window[0], trade_entry_window[1])
        if session_data.empty:
            logger.info(f"Skipping {date_val}: No data within entry window {trade_entry_window[0]}-{trade_entry_window[1]}.")
            continue

        # Determine the bar interval in minutes (based on the first two rows)
        try:
            interval_minutes = (session_data.index[1] - session_data.index[0]).seconds / 60.0
        except Exception as e:
            logger.error(f"Error determining interval for {date_val}: {e}")
            continue

        # Determine how many bars to use for the opening range
        bars_for_open_range = max(1, int(open_range_minutes // interval_minutes))
        if bars_for_open_range > len(session_data):
            logger.info(f"Insufficient bars on {date_val}: needed {bars_for_open_range}, available {len(session_data)}. Skipping day.")
            continue

        # Extract opening range data and compute high/low
        opening_range_data = session_data.iloc[:bars_for_open_range]
        or_high = opening_range_data["High"].max()
        or_low = opening_range_data["Low"].min()

        # Volume filter: check if total volume in the opening range exceeds the daily average (within session)
        or_volume = opening_range_data["Volume"].sum()
        day_avg_volume = session_data["Volume"].mean()
        volume_ok = or_volume > day_avg_volume

        trade_executed = False
        entry_price = None
        exit_price = None
        direction = None
        trade_entry_time = None

        # Iterate over bars after the opening range in the session
        for idx, row in session_data.iloc[bars_for_open_range:].iterrows():
            if not trade_executed:
                if use_volume_filter and not volume_ok:
                    logger.debug(f"{date_val}: Volume filter not met. Skipping trade.")
                    break

                # Check for breakout conditions
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

            # If in a trade, check exit conditions (stop-loss or time exit)
            if trade_executed:
                current_close = row["Close"]
                if stop_loss is not None:
                    if direction == "long" and current_close <= entry_price * (1 - stop_loss):
                        exit_price = entry_price * (1 - stop_loss)
                        break
                    elif direction == "short" and current_close >= entry_price * (1 + stop_loss):
                        exit_price = entry_price * (1 + stop_loss)
                        break

                if time_exit_minutes is not None and trade_entry_time is not None:
                    elapsed_minutes = (idx - trade_entry_time).seconds / 60.0
                    if elapsed_minutes >= time_exit_minutes:
                        exit_price = current_close
                        break

        # If a trade was opened but no exit condition was met, exit at session end
        if trade_executed and exit_price is None:
            exit_price = session_data.iloc[-1]["Close"]

        # Record the trade if executed
        if trade_executed:
            pnl = (exit_price - entry_price) if direction == "long" else (entry_price - exit_price)
            trade_record = {
                "date": date_val,
                "direction": direction,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "pnl": pnl
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

def run_opening_range_breakout_tests(ticker, days=30):
    """
    Runs a comprehensive set of test scenarios for a given ticker.
    
    Scenarios are generated by combining:
      - Opening range durations: 10, 30, 45 minutes.
      - Volume filter: Enabled or Disabled.
      - Stop-Loss: None or 1% (0.01).
      - Time-Based Exit: None or 150 minutes.
      
    This results in 3 x 2 x 2 x 2 = 24 scenarios.
    
    Returns:
        List[dict]: Each dict contains scenario name, parameter settings, and performance metrics.
    """
    results = []
    df = fetch_intraday_data(ticker, days=days, interval="15m")
    if df.empty:
        logger.warning(f"No intraday data available for {ticker}.")
        return results
    
    # Define the parameter options
    or_minutes_list = [10, 30, 45]
    volume_options = [False, True]
    stop_loss_options = [None, 0.01]      # None means no stop-loss; 0.01 is a 1% stop-loss
    time_exit_options = [None, 150]         # None means no time-based exit; 150 means exit after 150 minutes

    # Generate all combinations
    for or_minutes in or_minutes_list:
        for use_volume in volume_options:
            for stop_loss in stop_loss_options:
                for time_exit in time_exit_options:
                    # Build a descriptive name for the scenario
                    filters = []
                    if use_volume:
                        filters.append("Volume Filter")
                    if stop_loss is not None:
                        filters.append("Stop-Loss")
                    if time_exit is not None:
                        filters.append("Time Exit")
                    if not filters:
                        filters.append("No Filters")
                    filter_desc = " + ".join(filters)
                    scenario_name = f"OR {or_minutes}min, {filter_desc}"
                    
                    # Run the backtest for this scenario
                    trades_df = backtest_opening_range_breakout(
                        df,
                        open_range_minutes=or_minutes,
                        use_volume_filter=use_volume,
                        stop_loss=stop_loss,
                        time_exit_minutes=time_exit,
                        trade_entry_window=("09:30", "12:00")
                    )
                    
                    # Compute metrics from the trades
                    metrics = compute_metrics(trades_df)
                    
                    scenario_result = {
                        "scenario_name": scenario_name,
                        "open_range_minutes": or_minutes,
                        "use_volume_filter": use_volume,
                        "stop_loss": stop_loss,
                        "time_exit_minutes": time_exit,
                        "win_rate": metrics["win_rate"],
                        "profit_factor": metrics["profit_factor"],
                        "sharpe_ratio": metrics["sharpe_ratio"],
                        "max_drawdown": metrics["max_drawdown"],
                        "num_trades": metrics["num_trades"]
                    }
                    results.append(scenario_result)
    
    return results
