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
    """
    timestamp = datetime.now(_CHICAGO_TZ).strftime('%Y-%m-%d %H:%M:%S')

    # Fetch all tickers to scan (system-wide set)
    tickers = get_all_tickers()

    # Prepare data for each symbol
    data_dict = prepare_stock_data(tickers)

    # Detect events (Bollinger touches, etc.)
    touched_details = process_bollinger_touches(data_dict, mode='alert')

    return {
        "timestamp": timestamp,
        "alerts": touched_details or [],
    }

if __name__ == "__main__":
    # For quick local testing
    print(daily_scan())
