"""
data_fetcher.py
Facade for data fetchers (kept for backwards-compatible imports).
"""
from .data_fetcher_market import fetch_stock_data, fetch_stock_option_data
from .data_fetcher_fundamentals import fetch_stock_fundamentals, fetch_peers
from .data_fetcher_financials import fetch_financials

__all__ = [
    "fetch_stock_data",
    "fetch_stock_option_data",
    "fetch_stock_fundamentals",
    "fetch_peers",
    "fetch_financials",
]
