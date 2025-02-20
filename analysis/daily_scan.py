# daily_scan.py
import schedule
import time
from datetime import datetime
from .data_preparation import prepare_stock_data

TICKERS = [
    "TSLA","PLTR","SQ","NFLX","AMD","MU","CRWD","SHOP",
    "DFS","META","GS","NVDA","PYPL","SPOT","ABNB","CRM",
    "UBER","ZM","TGT","ADBE","AMZN","HD","PINS","AAPL",
    "BBY","V","COST","WMT","MSFT","DIS","SBUX","JPM",
    "LULU","MCD","T"
]

def check_bollinger_touch(data_dict):
    """
    Returns a list of tickers whose latest close
    touches or exceeds upper/lower Bollinger bands.
    """
    touched = []
    for symbol, df in data_dict.items():
        if len(df) < 1:
            continue
        last_row = df.iloc[-1]
        close_price = last_row['close']
        bb_upper = last_row['BB_upper']
        bb_lower = last_row['BB_lower']

        if close_price >= bb_upper or close_price <= bb_lower:
            touched.append(symbol)
    return touched

def daily_scan():
    print(f"Running daily scan at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    data_dict = prepare_stock_data(TICKERS)
    touched_symbols = check_bollinger_touch(data_dict)

    if touched_symbols:
        # In a real app, you'd send an email, Slack message, or push notification here
        print("ALERT: The following tickers touched Bollinger Bands:")
        print(", ".join(touched_symbols))
    else:
        print("No tickers touched the upper/lower Bollinger Bands today.")

# Schedule the job at 16:00 (4PM) Chicago time
# Chicago is usually US Central Time (CST/CDT).
# Adjust if needed, or rely on your server's local time zone.
schedule.every().day.at("16:00").do(daily_scan)