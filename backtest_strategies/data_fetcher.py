import pandas as pd
from analysis.data_fetcher import fetch_stock_data
from .logging_config import logger


def fetch_intraday_data(ticker: str, *, days: str = "30d", interval: str = "5m") -> pd.DataFrame:
    try:
        raw = fetch_stock_data(ticker, period=days, interval=interval)
        df = raw.get(ticker.upper())
        if df is None or df.empty:
            logger.warning(f"Empty dataset fetched for {ticker}.")
            return pd.DataFrame()
        logger.info(f"Fetched data for {ticker} successfully.")
    except Exception as e:
        logger.error(f"Error fetching data for {ticker}: {e}")
        return pd.DataFrame()

    # yfinance sometimes puts the timestamp in a column named “date” – move it to the index
    if "date" in df.columns:
        df.set_index("date", inplace=True)

    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, errors="coerce")
        df = df[~df.index.isna()]

    df.sort_index(inplace=True)
    return df
