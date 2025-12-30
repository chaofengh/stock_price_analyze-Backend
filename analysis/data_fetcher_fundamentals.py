"""
data_fetcher_fundamentals.py
Purpose: fetch fundamentals and peers from yfinance and Finnhub.
"""
import os
import finnhub
import yfinance as yf
from dotenv import load_dotenv
from .data_fetcher_utils import (
    normalize_symbol,
    symbol_candidates,
    safe_float,
    get_info_value,
    get_ticker_info,
    get_fast_info,
    get_price_from_history,
)

load_dotenv()

finnhub_api_key = os.environ.get("finnhub_api_key")
finnhub_client = finnhub.Client(api_key=finnhub_api_key)


def fetch_stock_fundamentals(symbol: str) -> dict:
    """
    Fetch fundamentals and compute key ratios.
    """
    symbol = normalize_symbol(symbol)

    def _extract(info, fast_info, price_fallback=None):
        def info_float(keys):
            return safe_float(get_info_value(info, keys))

        trailing_pe = info_float(["trailingPE", "trailingPe"])
        forward_pe = info_float(["forwardPE", "forwardPe"])
        trailing_eps = info_float(["trailingEps", "trailingEPS"])
        forward_eps = info_float(["forwardEps", "forwardEPS"])
        earnings_growth = info_float("earningsGrowth")

        current_price = info_float(["currentPrice", "regularMarketPrice", "previousClose"])
        if current_price is None:
            current_price = safe_float(fast_info.get("lastPrice"))
        if current_price is None:
            current_price = safe_float(fast_info.get("regularMarketPrice"))
        if current_price is None:
            current_price = safe_float(fast_info.get("previousClose"))
        if current_price is None and price_fallback is not None:
            current_price = price_fallback

        if trailing_pe is None and trailing_eps not in (None, 0) and current_price is not None:
            trailing_pe = current_price / trailing_eps
        if forward_pe is None and forward_eps not in (None, 0) and current_price is not None:
            forward_pe = current_price / forward_eps

        peg = None
        if forward_pe is not None and earnings_growth not in (None, 0):
            growth_pct = earnings_growth * 100 if -1 < earnings_growth < 1 else earnings_growth
            if growth_pct:
                peg = forward_pe / growth_pct
        if peg is None:
            peg = info_float("trailingPegRatio")

        pgi = None
        if forward_pe is not None and trailing_pe not in (None, 0):
            pgi = forward_pe / trailing_pe
        elif trailing_eps not in (None, 0) and forward_eps not in (None, 0):
            pgi = trailing_eps / forward_eps

        trailingPEG = info_float("trailingPegRatio")
        dividendYield = info_float("dividendYield")
        beta = info_float("beta")
        marketCap = info_float("marketCap")
        if marketCap is None:
            marketCap = safe_float(fast_info.get("marketCap"))
        shares_outstanding = info_float("sharesOutstanding")
        if marketCap is None and current_price is not None and shares_outstanding:
            marketCap = current_price * shares_outstanding
        priceToBook = info_float("priceToBook")
        forwardEPS = forward_eps
        trailingEPS = trailing_eps
        debtToEquity = info_float("debtToEquity")

        return {
            "trailingPE": trailing_pe,
            "forwardPE": forward_pe,
            "PEG": peg,
            "PGI": pgi,
            "trailingPEG": trailingPEG,
            "dividendYield": dividendYield,
            "beta": beta,
            "marketCap": marketCap,
            "priceToBook": priceToBook,
            "forwardEPS": forwardEPS,
            "trailingEPS": trailing_eps,
            "debtToEquity": debtToEquity,
        }

    def _load(symbol_override):
        try:
            ticker = yf.Ticker(symbol_override)
            info = get_ticker_info(ticker)
            fast_info = get_fast_info(ticker)
            price_fallback = get_price_from_history(ticker)
        except Exception:
            return {}
        return _extract(info, fast_info, price_fallback=price_fallback)

    def _is_empty(payload):
        if not payload:
            return True
        for key in (
            "trailingPE",
            "forwardPE",
            "PEG",
            "PGI",
            "dividendYield",
            "beta",
            "marketCap",
        ):
            if payload.get(key) is not None:
                return False
        return True

    fundamentals = {}
    for candidate in symbol_candidates(symbol):
        fundamentals = _load(candidate)
        if not _is_empty(fundamentals):
            break

    return fundamentals


def fetch_peers(symbol: str) -> list:
    """
    Fetch peer tickers for a given symbol using Finnhub.
    """
    for candidate in symbol_candidates(symbol):
        try:
            peers = finnhub_client.company_peers(candidate)
        except Exception:
            peers = None
        if isinstance(peers, list) and peers:
            return peers
    return []
