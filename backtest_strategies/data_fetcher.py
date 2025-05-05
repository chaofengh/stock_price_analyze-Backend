# backtest_strategies/data_fetcher.py

import pandas as pd
from analysis.data_fetcher import fetch_stock_data
from .logging_config import logger


# ─────────── backtest_strategies/data_fetcher.py (patch) ───────────
def fetch_intraday_data(ticker: str, *, days='30d', interval='5m') -> pd.DataFrame:

    try:
        raw = fetch_stock_data(ticker, period=days, interval=interval)
        df  = raw.get(ticker.upper())
        if df is None or df.empty:
            logger.warning(f"Empty dataset fetched for {ticker}.")
            return pd.DataFrame()
        logger.info(f"Fetched data for {ticker} successfully.")
    except Exception as e:
        logger.error(f"Error fetching data for {ticker}: {e}")
        return pd.DataFrame()

    # ---------- NEW: ensure we have a DatetimeIndex ----------
    if "date" in df.columns:
        df.set_index("date", inplace=True)
    if not isinstance(df.index, pd.DatetimeIndex):
        # last‑ditch attempt to coerce
        df.index = pd.to_datetime(df.index, errors="coerce")
        df.dropna(subset=[df.index.name or "index"], inplace=True)

    df.sort_index(inplace=True)

    # ---------- timezone & helper columns ----------
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")

    df["timestamp_utc"] = df.index.strftime("%Y-%m-%dT%H:%M:%SZ")
    return df

