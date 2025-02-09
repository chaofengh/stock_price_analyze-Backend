# data_preparation.py
import pandas as pd
from .data_fetcher import fetch_stock_data
from .indicators import compute_bollinger_bands, compute_rsi

def prepare_stock_data(symbol: str) -> pd.DataFrame:
    data = fetch_stock_data(symbol)
    data = compute_bollinger_bands(data)
    data = compute_rsi(data)
    return data

def get_trading_period(data) -> (str, float, float):
    n = len(data)
    start_date_str = data.loc[0, 'date'].strftime('%Y-%m-%d')
    end_date_str   = data.loc[n-1, 'date'].strftime('%Y-%m-%d')
    analysis_period = f"{start_date_str} to {end_date_str}"
    initial_price = float(data.loc[0, 'close'])
    final_price = float(data.loc[n-1, 'close'])
    return analysis_period, initial_price, final_price
