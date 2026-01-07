import yfinance as yf

from .data_fetcher_financials import fetch_financials
from .data_fetcher_fundamentals_extract import extract_fundamentals


def load_fundamentals(symbol_override, include_alpha=True):
    try:
        ticker = yf.Ticker(symbol_override)
        info = ticker.info or {}
        try:
            fast_info = ticker.fast_info or {}
        except Exception:
            fast_info = {}
        try:
            income_stmt = ticker.financials
        except Exception:
            income_stmt = None
        try:
            balance_sheet = ticker.balance_sheet
        except Exception:
            balance_sheet = None
        try:
            cashflow = ticker.cashflow
        except Exception:
            cashflow = None
        try:
            income_quarterly = ticker.quarterly_financials
        except Exception:
            income_quarterly = None
        try:
            balance_quarterly = ticker.quarterly_balance_sheet
        except Exception:
            balance_quarterly = None
        try:
            cashflow_quarterly = ticker.quarterly_cashflow
        except Exception:
            cashflow_quarterly = None
        alpha_financials = {}
        if include_alpha:
            try:
                alpha_financials = fetch_financials(
                    symbol_override,
                    statements=["income_statement", "balance_sheet", "cash_flow"],
                )
            except Exception:
                alpha_financials = {}
    except Exception:
        return {}

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
