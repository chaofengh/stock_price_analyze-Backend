import yfinance as yf


ticker = yf.Ticker('amd')
info = ticker.info
print(info)