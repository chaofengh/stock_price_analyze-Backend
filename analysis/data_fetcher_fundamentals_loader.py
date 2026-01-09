import yfinance as yf

from .data_fetcher_financials import fetch_financials
from .data_fetcher_fundamentals_extract import extract_fundamentals
from .data_fetcher_utils import get_fast_info, get_price_from_history, get_ticker_info


def _safe_attr(ticker, attr):
    try:
        return getattr(ticker, attr)
    except Exception:
        return None


def load_fundamentals(symbol_override, include_alpha=True):
    ticker = yf.Ticker(symbol_override)
    info = get_ticker_info(ticker)
    fast_info = get_fast_info(ticker)
    price = get_price_from_history(ticker)
    if price is not None:
        info.setdefault("currentPrice", price)
        fast_info.setdefault("lastPrice", price)

    income_stmt = _safe_attr(ticker, "financials")
    balance_sheet = _safe_attr(ticker, "balance_sheet")
    cashflow = _safe_attr(ticker, "cashflow")
    income_quarterly = _safe_attr(ticker, "quarterly_financials")
    balance_quarterly = _safe_attr(ticker, "quarterly_balance_sheet")
    cashflow_quarterly = _safe_attr(ticker, "quarterly_cashflow")
    alpha_financials = {}
    if include_alpha:
        try:
            alpha_financials = fetch_financials(
                symbol_override,
                statements=["income_statement", "balance_sheet", "cash_flow"],
            )
        except Exception:
            alpha_financials = {}

    statements = {
        "income": income_stmt,
        "balance": balance_sheet,
        "cashflow": cashflow,
        "income_quarterly": income_quarterly,
        "balance_quarterly": balance_quarterly,
        "cashflow_quarterly": cashflow_quarterly,
        "alpha_financials": alpha_financials,
    }
    return extract_fundamentals(info, fast_info, statements=statements)
