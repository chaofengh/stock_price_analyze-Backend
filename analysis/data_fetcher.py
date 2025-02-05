import os
import pandas as pd
from alpha_vantage.timeseries import TimeSeries


api_key = os.environ.get("alpha_vantage_api_key")

def fetch_stock_data(symbol: str, outputsize: str = 'compact') -> pd.DataFrame:
    """
    Fetches daily stock data from Alpha Vantage and performs initial cleanup.
    """
    
    if not api_key:
        raise ValueError("Missing 'alpha_vantage_api_key' in environment")
    
    ts = TimeSeries(key=api_key, output_format='pandas')
    data, _ = ts.get_daily(symbol=symbol, outputsize=outputsize)
    
    data.sort_index(inplace=True)
    data.rename(columns={
        '1. open': 'open',
        '2. high': 'high',
        '3. low':  'low',
        '4. close': 'close',
        '5. volume': 'volume'
    }, inplace=True)
    
    data.reset_index(inplace=True)
    data.rename(columns={'index': 'date'}, inplace=True)
    data['date'] = pd.to_datetime(data['date'])
    return data

def fetch_stock_fundamentals(symbol):
    """
    Fetches stock fundamentals (e.g., PE ratio and PEG) from Alpha Vantage.
    Replace 'YOUR_API_KEY' with your actual API key.
    """
    url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={symbol}&apikey={api_key}"
    
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception("Error fetching fundamentals from Alpha Vantage")
    
    data = response.json()
    try:
        pe_ratio = float(data.get("PERatio", None)) if data.get("PERatio") is not None else None
    except Exception:
        pe_ratio = None

    try:
        peg = float(data.get("PEGRatio", None)) if data.get("PEGRatio") is not None else None
    except Exception:
        peg = None
        
    return {
        "PE_ratio": pe_ratio,
        "PEG": peg,
    }

