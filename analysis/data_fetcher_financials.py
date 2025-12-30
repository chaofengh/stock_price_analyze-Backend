"""
data_fetcher_financials.py
Purpose: fetch and cache financial statements from Alpha Vantage.
"""
import os
import requests
from dotenv import load_dotenv
from utils.ttl_cache import TTLCache
from .data_fetcher_utils import normalize_symbol
from .financials_alpha import is_alpha_vantage_error, has_financial_reports
from .financials_helpers import (
    compute_partial_year_reports,
    compute_annual_from_quarters,
)
from .financials_yfinance import build_income_annual_from_yfinance

load_dotenv()

alpha_vantage_api_key = os.environ.get("alpha_vantage_api_key")

_FINANCIALS_CACHE = TTLCache(ttl_seconds=60 * 60 * 6, max_size=512)
_FINANCIALS_EMPTY_CACHE = TTLCache(ttl_seconds=60 * 5, max_size=512)
_NO_DATA = object()


def fetch_financials(symbol: str, statements=None) -> dict:
    """
    Fetch financial statements (income, balance sheet, cash flow).
    """
    if not alpha_vantage_api_key:
        raise ValueError("Missing 'alpha_vantage_api_key' in environment")

    symbol = normalize_symbol(symbol)

    valid_types = {
        "income_statement": "INCOME_STATEMENT",
        "balance_sheet": "BALANCE_SHEET",
        "cash_flow": "CASH_FLOW",
    }

    if statements is None:
        requested_types = list(valid_types.keys())
    else:
        if isinstance(statements, str):
            requested_types = [statements]
        elif isinstance(statements, list):
            requested_types = statements
        else:
            raise ValueError("`statements` must be a string or a list of strings")

    for statement in requested_types:
        if statement not in valid_types:
            raise ValueError(
                f"Invalid statement type: {statement}. Valid options are: {list(valid_types.keys())}"
            )

    financials = {}
    for statement in requested_types:
        cache_key = (symbol, statement)
        cached = _FINANCIALS_CACHE.get(cache_key, _NO_DATA)
        if cached is not _NO_DATA:
            financials[statement] = cached
            continue
        cached_empty = _FINANCIALS_EMPTY_CACHE.get(cache_key, _NO_DATA)
        if cached_empty is not _NO_DATA:
            financials[statement] = cached_empty
            continue

        function_name = valid_types[statement]
        url = (
            "https://www.alphavantage.co/query"
            f"?function={function_name}&symbol={symbol}&apikey={alpha_vantage_api_key}"
        )
        try:
            response = requests.get(url, timeout=8)
            if response.status_code != 200:
                raise Exception(f"Error fetching {statement} data: {response.status_code}")
            data = response.json()
        except Exception:
            if statement == "income_statement":
                data = {}
            else:
                raise
        if not isinstance(data, dict):
            data = {}

        if "annualReports" in data:
            if isinstance(data["annualReports"], list):
                data["annualReports"] = sorted(
                    data["annualReports"],
                    key=lambda x: x.get("fiscalDateEnding", ""),
                    reverse=True,
                )[:3]
            else:
                data["annualReports"] = []

        if "quarterlyReports" in data:
            if isinstance(data["quarterlyReports"], list):
                data["quarterlyReports"] = sorted(
                    data["quarterlyReports"],
                    key=lambda x: x.get("fiscalDateEnding", ""),
                    reverse=True,
                )[:12]
            else:
                data["quarterlyReports"] = []

            partial_year = compute_partial_year_reports(data["quarterlyReports"])
            if partial_year:
                data["partialYearReports"] = partial_year

            if statement == "income_statement" and not data.get("annualReports"):
                computed_annual = compute_annual_from_quarters(data["quarterlyReports"])
                if computed_annual:
                    data["annualReports"] = computed_annual[:3]

        if statement == "income_statement":
            if is_alpha_vantage_error(data) or not data.get("annualReports"):
                fallback_annual = build_income_annual_from_yfinance(symbol)
                if fallback_annual:
                    data.pop("Note", None)
                    data.pop("Error Message", None)
                    data.pop("Information", None)
                    data["annualReports"] = fallback_annual[:3]

        data["symbol"] = symbol
        if has_financial_reports(data):
            _FINANCIALS_CACHE.set(cache_key, data)
        else:
            _FINANCIALS_EMPTY_CACHE.set(cache_key, data)
        financials[statement] = data

    return financials
