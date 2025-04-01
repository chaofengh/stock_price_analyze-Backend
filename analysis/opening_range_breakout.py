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
# Data Fetching and Preprocessing
# ---------------------------
def fetch_intraday_data(ticker, days=30, interval="15m", target_timezone="US/Eastern"):
    try:
        df = yf.download(ticker, period=f"{days}d", interval=interval, auto_adjust=True, progress=False)
        if df.empty:
            logger.warning(f"Empty dataset fetched for {ticker}.")
            return pd.DataFrame()
        logger.info(f"Fetched data for {ticker} successfully.")
    except Exception as e:
        logger.error(f"Error fetching data for {ticker}: {e}")
        return pd.DataFrame()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    
    if df.index.tzinfo is None:
        df.index = df.index.tz_localize("UTC")
    try:
        df.index = df.index.tz_convert(target_timezone)
    except Exception as e:
        logger.error(f"Timezone conversion error for {ticker}: {e}")
        return pd.DataFrame()

    try:
        df = df.between_time("09:30", "16:00")
    except Exception as e:
        logger.error(f"Error filtering market hours for {ticker}: {e}")

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
    if df.empty:
        logger.warning("Empty DataFrame received for backtesting.")
        return pd.DataFrame()
    
    df = df.copy()
    df["date"] = df.index.date
    grouped = df.groupby("date")
    all_trades = []

    for date_val, day_data in grouped:
        day_data = day_data.sort_index()

        full_day_data = day_data.between_time("09:30", "16:00")
        if full_day_data.empty:
            logger.info(f"Skipping {date_val}: No data for full session.")
            continue

        entry_data = full_day_data.between_time(trade_entry_window[0], trade_entry_window[1])
        if entry_data.empty:
            logger.info(f"Skipping {date_val}: No data in entry window {trade_entry_window[0]}-{trade_entry_window[1]}.")
            continue

        try:
            interval_minutes = (entry_data.index[1] - entry_data.index[0]).seconds / 60.0
        except Exception as e:
            logger.error(f"Error determining interval for {date_val}: {e}")
            continue

        bars_for_open_range = max(1, int(open_range_minutes // interval_minutes))
        if bars_for_open_range > len(entry_data):
            logger.info(f"Skipping {date_val}: insufficient bars for open range, needed {bars_for_open_range}, available {len(entry_data)}.")
            continue

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

        for idx, row in full_day_data.iterrows():
            if not trade_executed and idx.time() <= datetime.time(12, 0):
                if use_volume_filter and not volume_ok:
                    logger.debug(f"{date_val}: Volume filter not met. Skipping trade.")
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

            if trade_executed:
                current_close = row["Close"]
                if stop_loss is not None:
                    if direction == "long" and current_close <= entry_price * (1 - stop_loss):
                        exit_price = entry_price * (1 - stop_loss)
                        trade_exit_time = idx
                        break
                    elif direction == "short" and current_close >= entry_price * (1 + stop_loss):
                        exit_price = entry_price * (1 + stop_loss)
                        trade_exit_time = idx
                        break
                if time_exit_minutes is not None and trade_entry_time is not None:
                    elapsed_minutes = (idx - trade_entry_time).seconds / 60.0
                    if elapsed_minutes >= time_exit_minutes:
                        exit_price = current_close
                        trade_exit_time = idx
                        break

        if trade_executed and exit_price is None:
            exit_price = full_day_data.iloc[-1]["Close"]
            trade_exit_time = full_day_data.index[-1]

        if trade_executed:
            pnl = (exit_price - entry_price) if direction == "long" else (entry_price - exit_price)
            trade_record = {
                "date": date_val,
                "direction": direction,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "pnl": pnl,
                "entry_time": trade_entry_time.isoformat() if trade_entry_time else None,
                "exit_time": trade_exit_time.isoformat() if trade_exit_time else None
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
    results = []
    df = fetch_intraday_data(ticker, days=days, interval="15m")
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
                    filters = []
                    if use_volume:
                        filters.append("Volume Filter")
                    if stop_loss is not None:
                        filters.append("Stop-Loss")
                    if time_exit is not None:
                        filters.append(f"Time Exit ({time_exit}min)")
                    if not filters:
                        filters.append("No Filters")
                    filter_desc = " + ".join(filters)
                    scenario_name = f"OR {or_minutes}min, {filter_desc}"
                    
                    trades_df = backtest_opening_range_breakout(
                        df,
                        open_range_minutes=or_minutes,
                        use_volume_filter=use_volume,
                        stop_loss=stop_loss,
                        time_exit_minutes=time_exit,
                        trade_entry_window=("09:30", "12:00")
                    )
                    
                    metrics = compute_metrics(trades_df)
                    daily_trades = trades_df.to_dict(orient="records")
                    net_pnl = trades_df["pnl"].sum() if not trades_df.empty else 0

                    
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
                        "num_trades": metrics["num_trades"],
                        "daily_trades": daily_trades,
                        "net_pnl": round(net_pnl, 2)
                    }
                    results.append(scenario_result)
    
    # Process intraday data for the full period.
    intraday_df = df.reset_index()
    # Ensure the date column is correctly named. If not, rename the first column.
    if 'date' not in intraday_df.columns:
        intraday_df = intraday_df.rename(columns={intraday_df.columns[0]: 'date'})
    intraday_df = intraday_df.rename(columns={
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Close': 'close',
        'Volume': 'volume'
    })
    intraday_df['date'] = intraday_df['date'].apply(lambda d: d.isoformat())
    intraday_data = intraday_df.to_dict(orient='records')
    
    return {"scenarios": results, "intraday_data": intraday_data}
