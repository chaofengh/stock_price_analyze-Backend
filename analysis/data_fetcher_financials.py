"""
data_fetcher_financials.py
Purpose: fetch and cache financial statements from Alpha Vantage.
"""
import os
import threading
import time
from collections import deque

import requests
from dotenv import load_dotenv
from utils.ttl_cache import TTLCache
from database.financials_repository import get_financial_statement, upsert_financial_statement
from .data_fetcher_utils import normalize_symbol
from .financials_alpha import is_alpha_vantage_error, has_financial_reports
from .financials_helpers import compute_annual_from_quarters, compute_partial_year_reports
from .financials_yfinance import build_income_annual_from_yfinance

load_dotenv()

alpha_vantage_api_key = os.environ.get("alpha_vantage_api_key")

_FINANCIALS_CACHE = TTLCache(ttl_seconds=60 * 60 * 6, max_size=512)
_FINANCIALS_EMPTY_CACHE = TTLCache(ttl_seconds=60 * 5, max_size=512)
_NO_DATA = object()

_AV_RATE_WINDOW_SECONDS = int(os.environ.get("ALPHA_VANTAGE_WINDOW_SECONDS", "60"))
_AV_RATE_MAX_CALLS = int(os.environ.get("ALPHA_VANTAGE_MAX_CALLS_PER_WINDOW", "4"))
_AV_DISABLE_SECONDS = int(
    os.environ.get("ALPHA_VANTAGE_DISABLE_SECONDS", str(60 * 60 * 24))
)
_AV_CALL_TIMES = deque()
_AV_DISABLED_UNTIL = 0.0
_AV_RATE_LOCK = threading.Lock()

_build_income_annual_from_yfinance = build_income_annual_from_yfinance


def _is_alpha_vantage_rate_limit(payload) -> bool:
    if not isinstance(payload, dict):
        return False
    note = payload.get("Note") or payload.get("Information")
    if not note:
        return False
    note_lower = str(note).lower()
    return "api call frequency" in note_lower or "call frequency" in note_lower


def _alpha_vantage_allow_request() -> bool:
    now = time.time()
    with _AV_RATE_LOCK:
        if _AV_DISABLED_UNTIL and now < _AV_DISABLED_UNTIL:
            return False
        if _AV_RATE_MAX_CALLS <= 0:
            return False
        if _AV_RATE_WINDOW_SECONDS > 0:
            while _AV_CALL_TIMES and (now - _AV_CALL_TIMES[0]) > _AV_RATE_WINDOW_SECONDS:
                _AV_CALL_TIMES.popleft()
            if len(_AV_CALL_TIMES) >= _AV_RATE_MAX_CALLS:
                return False
        _AV_CALL_TIMES.append(now)
        return True


def _alpha_vantage_disable_for_window():
    global _AV_DISABLED_UNTIL
    if _AV_DISABLE_SECONDS <= 0:
        return
    now = time.time()
    with _AV_RATE_LOCK:
        disabled_until = now + _AV_DISABLE_SECONDS
        if disabled_until > _AV_DISABLED_UNTIL:
            _AV_DISABLED_UNTIL = disabled_until

def fetch_financials(symbol: str, statements=None) -> dict:
    """
    Fetch financial statements (income, balance sheet, cash flow).
    """
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

        db_payload = None
        try:
            db_payload = get_financial_statement(symbol, statement)
        except Exception:
            db_payload = None

        if isinstance(db_payload, dict):
            if db_payload.get("symbol") is None:
                db_payload["symbol"] = symbol
            if has_financial_reports(db_payload):
                _FINANCIALS_CACHE.set(cache_key, db_payload)
            else:
                _FINANCIALS_EMPTY_CACHE.set(cache_key, db_payload)
            financials[statement] = db_payload
            continue

        if not alpha_vantage_api_key:
            raise ValueError("Missing 'alpha_vantage_api_key' in environment")

        function_name = valid_types[statement]
        url = (
            "https://www.alphavantage.co/query"
            f"?function={function_name}&symbol={symbol}&apikey={alpha_vantage_api_key}"
        )
        fetched = False
        source = "alpha_vantage"
        data = {}
        if _alpha_vantage_allow_request():
            try:
                response = requests.get(url, timeout=8)
                if response.status_code != 200:
                    raise Exception(f"Error fetching {statement} data: {response.status_code}")
                data = response.json()
                fetched = True
            except Exception:
                if statement == "income_statement":
                    data = {}
                else:
                    raise
        if not isinstance(data, dict):
            data = {}
        if _is_alpha_vantage_rate_limit(data):
            _alpha_vantage_disable_for_window()

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
                fallback_annual = _build_income_annual_from_yfinance(symbol)
                if fallback_annual:
                    data.pop("Note", None)
                    data.pop("Error Message", None)
                    data.pop("Information", None)
                    data["annualReports"] = fallback_annual[:3]
                    fetched = True
                    source = "yfinance"

        data["symbol"] = symbol
        if has_financial_reports(data):
            _FINANCIALS_CACHE.set(cache_key, data)
        else:
            _FINANCIALS_EMPTY_CACHE.set(cache_key, data)

        if fetched and not is_alpha_vantage_error(data):
            try:
                upsert_financial_statement(symbol, statement, data, source=source)
            except Exception:
                pass
        financials[statement] = data

    return financials
