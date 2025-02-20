import yfinance as yf
import time
from yfinance.exceptions import YFRateLimitError

symbols = ["AAPL", "MSFT", "GOOGL"]  # up to 39
data = yf.download(
    tickers=" ".join(symbols),   # or pass a list
    period="6mo",                # e.g., 1 month
    interval="1d",               # daily data
    group_by='ticker'
)

ticker = yf.Ticker('AMD')

for attempt in range(2):  # try twice
    try:
        info = ticker.info
        break
    except YFRateLimitError:
        print("Rate limited, waiting before retrying...")
        time.sleep(60)
else:
    raise Exception("Still rate limited after retrying.")

print('info',info)
print('data',data)