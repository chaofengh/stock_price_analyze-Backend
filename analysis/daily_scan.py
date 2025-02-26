# daily_scan.py
from datetime import datetime
from .data_preparation import prepare_stock_data

TICKERS = [
    "TSLA", "PLTR", "SQ", "NFLX", "AMD", "MU", "CRWD", "SHOP",
    "DFS", "META", "GS", "NVDA", "PYPL", "SPOT", "ABNB", "CRM",
    "UBER", "ZM", "TGT", "ADBE", "AMZN", "HD", "PINS", "AAPL",
    "BBY", "V", "COST", "WMT", "MSFT", "DIS", "SBUX", "JPM",
    "LULU", "MCD", "T"
]

def check_bollinger_touch(data_dict):
    """
    Return a list of dicts with detailed info if the latest close 
    touches or exceeds Bollinger bands.
    """
    touched_details = []
    for symbol, df in data_dict.items():
        if len(df) < 1:
            continue
        last_row = df.iloc[-1]
        close_price = last_row['close']
        bb_upper = last_row['BB_upper']
        bb_lower = last_row['BB_lower']

        # Check for touch/exceed
        if close_price >= bb_upper or close_price <= bb_lower:
            side = "Upper" if close_price >= bb_upper else "Lower"
            touched_details.append({
                "symbol": symbol,
                "close_price": float(close_price),
                "bb_upper": float(bb_upper),
                "bb_lower": float(bb_lower),
                "touched_side": side
            })
    return touched_details

def daily_scan():
    """
    Perform the daily scan, returning a dict with:
      - timestamp (str)
      - alerts (list of dict)
    """
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data_dict = prepare_stock_data(TICKERS)
    touched_details = check_bollinger_touch(data_dict)

    return {
        "timestamp": timestamp,
        "alerts": touched_details
    }

if __name__ == "__main__":
    # For quick local testing
    result = daily_scan()
    print(result)
