# analysis/daily_scan.py
from datetime import datetime
import pytz

from .data_preparation import prepare_stock_data
from database.ticker_repository import get_all_tickers
from .event_detection import process_bollinger_touches

_CHICAGO_TZ = pytz.timezone("America/Chicago")

def daily_scan():
    """
    Perform the daily scan, returning a dict with:
      - timestamp (str, America/Chicago)
      - alerts (list[dict])
    The official scheduler runs this at 16:02 CT.
    """
    timestamp = datetime.now(_CHICAGO_TZ).strftime('%Y-%m-%d %H:%M:%S')

    tickers = get_all_tickers()
    data_dict = prepare_stock_data(tickers)
    touched_details = process_bollinger_touches(data_dict, mode='alert')

    return {
        "timestamp": timestamp,
        "alerts": touched_details or [],
    }
