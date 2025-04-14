# backtest_strategies/data_fetcher.py

import yfinance as yf
import pandas as pd
from .logging_config import logger

def fetch_intraday_data(ticker, days=30, interval="5m"):
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

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)

    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")

    df["timestamp_utc"] = df.index.strftime("%Y-%m-%dT%H:%M:%SZ")
    return df
