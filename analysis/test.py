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
    print(df)
    return df
fetch_intraday_data("TSLA", days=1, interval="5m")