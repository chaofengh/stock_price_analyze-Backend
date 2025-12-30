"""
daily_scan.py
Purpose: run the daily Bollinger touch scan across all tracked tickers.
Pseudocode:
1) Load all tickers.
2) Fetch data + indicators.
3) Detect band touches on the latest candle for each symbol.
"""
from datetime import datetime
import pytz

from .data_preparation import prepare_stock_data
from database.ticker_repository import get_all_tickers
from .event_detection import process_bollinger_touches

_CHICAGO_TZ = pytz.timezone("America/Chicago")

def daily_scan():
    """
    Perform the daily scan and return a JSON-friendly payload.
    """
    timestamp = datetime.now(_CHICAGO_TZ).strftime('%Y-%m-%d %H:%M:%S')

    tickers = get_all_tickers()
    data_dict = prepare_stock_data(tickers)
    touched_details = process_bollinger_touches(data_dict, mode='alert')

    return {
        "timestamp": timestamp,
        "alerts": touched_details or [],
    }
