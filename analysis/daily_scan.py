#daily_scan.py
from datetime import datetime
import pytz
from .data_preparation import prepare_stock_data
from database.ticker_repository import get_all_tickers
from .event_detection import process_bollinger_touches

def daily_scan():
    """
    Perform the daily scan, returning a dict with:
      - timestamp (str)
      - alerts (list of dict)
    """
    # Now using a timezone-aware datetime in UTC.
    timestamp = datetime.now(pytz.timezone("America/Chicago")).strftime('%Y-%m-%d %H:%M:%S')
    
    # Fetch the ticker symbols from the DB
    tickers = get_all_tickers()
    
    # Prepare data for each symbol
    data_dict = prepare_stock_data(tickers)
    
    # Check for Bollinger touches
    touched_details = process_bollinger_touches(data_dict, mode='alert')
    print(timestamp)

    return {
        "timestamp": timestamp,
        "alerts": touched_details
    }

if __name__ == "__main__":
    # For quick local testing
    result = daily_scan()
