# data_preparation.py
import pandas as pd
from .data_fetcher import fetch_stock_data
from .indicators import compute_bollinger_bands, compute_rsi

def prepare_stock_data(symbols):
    """
    Fetches data for multiple symbols, then computes Bollinger Bands & RSI.
    Returns a dictionary of DataFrames keyed by symbol.
    """
    data_dict = fetch_stock_data(symbols)

    for symbol, df in data_dict.items():
        df = compute_bollinger_bands(df)
        df = compute_rsi(df)
        data_dict[symbol] = df

    return data_dict

def get_trading_period(data):
    """
    Given a single DataFrame (with 'date' and 'close'),
    returns a string of the start-end date and initial/final prices.
    """
    n = len(data)
    if n == 0:
        return "No data", None, None

    start_date_str = data.loc[0, 'date'].strftime('%Y-%m-%d')
    end_date_str   = data.loc[n-1, 'date'].strftime('%Y-%m-%d')
    analysis_period = f"{start_date_str} to {end_date_str}"

    initial_price = float(data.loc[0, 'close'])
    final_price = float(data.loc[n-1, 'close'])
    return analysis_period, initial_price, final_price
