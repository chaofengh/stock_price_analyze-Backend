"""
data_preparation.py
Purpose: load raw price data and attach indicators needed downstream.
Pseudocode:
1) Fetch OHLCV data for symbol(s).
2) Add Bollinger Bands (and optional RSI).
3) Return a dict of DataFrames keyed by symbol.
"""
import pandas as pd
from .data_fetcher import fetch_stock_data
from .indicators import compute_bollinger_bands, compute_rsi

def prepare_stock_data(
    symbols,
    include_rsi: bool = True,
    period: str = "4mo",
    interval: str = "1d",
) -> dict:
    """
    Fetch data and compute indicators for one or many symbols.
    """
    data_dict = fetch_stock_data(symbols, period=period, interval=interval)

    for symbol, df in data_dict.items():
        if df is None or df.empty or "close" not in df.columns:
            data_dict[symbol] = df
            continue
        df = compute_bollinger_bands(df)
        if include_rsi:
            df = compute_rsi(df)
        data_dict[symbol] = df

    return data_dict

def get_trading_period(data):
    """
    Return a readable date range plus start/end prices.
    """
    n = len(data)
    if n == 0:
        return "No data", None, None

    start_date_str = data['date'].iloc[0].strftime('%Y-%m-%d')
    end_date_str   = data['date'].iloc[-1].strftime('%Y-%m-%d')
    analysis_period = f"{start_date_str} to {end_date_str}"

    initial_price = float(data['close'].iloc[0])
    final_price = float(data['close'].iloc[-1])
    return analysis_period, initial_price, final_price
