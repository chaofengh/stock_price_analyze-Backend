
import yfinance as yf

ticker=yf.Ticker('nvda')
info=ticker.info


trailing_pe = info.get("trailingPE")
if trailing_pe is not None:
    pe_ratio = float(trailing_pe)
else:
    pe_ratio = None

print(pe_ratio)


print(info)