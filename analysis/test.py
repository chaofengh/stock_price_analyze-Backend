import yfinance as yf

# Fetch recent historical data (not real-time)
data = yf.download("AAPL", period="1d", interval="1m")
print(data.tail())
