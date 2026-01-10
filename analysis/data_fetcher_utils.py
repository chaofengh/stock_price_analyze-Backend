import numpy as np
import pandas as pd

from .financials_alpha import (
    has_financial_reports as _has_financial_reports_impl,
    is_alpha_vantage_error as _is_alpha_vantage_error_impl,
)
from .financials_helpers import (
    compute_annual_from_quarters as _compute_annual_from_quarters_impl,
    compute_partial_year_reports as _compute_partial_year_reports_impl,
    decimal_to_string as _decimal_to_string_impl,
    month_to_quarter as _month_to_quarter_impl,
    safe_decimal as _safe_decimal_impl,
)
from .financials_yfinance import normalize_line_name as _normalize_line_name_impl


def normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper() if symbol else ""


def symbol_candidates(symbol: str) -> list:
    sym = normalize_symbol(symbol)
    if not sym:
        return []
    candidates = [sym]
    if "-" in sym:
        candidates.append(sym.replace("-", "."))
    if "." in sym:
        candidates.append(sym.replace(".", "-"))
    if "/" in sym:
        candidates.append(sym.replace("/", "-"))
    seen = set()
    unique = []
    for candidate in candidates:
        if candidate and candidate not in seen:
            seen.add(candidate)
            unique.append(candidate)
    return unique


def safe_float(value):
    if value is None:
        return None
    try:
        if isinstance(value, str):
            cleaned = value.strip()
            if not cleaned or cleaned.lower() in ("none", "nan", "na", "n/a", "null"):
                return None
            value = cleaned.replace(",", "")
        value = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(value):
        return None
    return value


def get_info_value(info, keys):
    if not isinstance(info, dict):
        return None
    if isinstance(keys, str):
        keys = [keys]
    for key in keys:
        if key in info:
            value = info.get(key)
            if value is not None:
                return value
    return None


def _is_missing(value) -> bool:
    if value is None:
        return True
    if isinstance(value, (float, np.floating)):
        return not np.isfinite(value)
    return False


def _merge_info(primary, secondary):
    merged = {}
    if isinstance(primary, dict):
        for key, value in primary.items():
            if not _is_missing(value):
                merged[key] = value
    if isinstance(secondary, dict):
        for key, value in secondary.items():
            if not _is_missing(value) and key not in merged:
                merged[key] = value
    return merged


def get_ticker_info(ticker):
    info_from_get = {}
    info_from_prop = {}
    try:
        if hasattr(ticker, "get_info"):
            info_from_get = ticker.get_info() or {}
    except Exception:
        info_from_get = {}
    try:
        info_from_prop = ticker.info or {}
    except Exception:
        info_from_prop = {}
    if not info_from_get and not info_from_prop:
        return {}
    return _merge_info(info_from_get, info_from_prop)


def get_fast_info(ticker):
    fast_from_get = {}
    fast_from_prop = {}
    try:
        if hasattr(ticker, "get_fast_info"):
            fast_from_get = ticker.get_fast_info() or {}
    except Exception:
        fast_from_get = {}
    try:
        fast_from_prop = ticker.fast_info or {}
    except Exception:
        fast_from_prop = {}
    if not fast_from_get and not fast_from_prop:
        return {}
    return _merge_info(fast_from_get, fast_from_prop)


def get_price_from_history(ticker):
    try:
        hist = ticker.history(period="1d", interval="1d")
    except Exception:
        return None
    if hist is None or getattr(hist, "empty", True):
        return None
    for col in ("Close", "Adj Close"):
        if col in hist.columns:
            value = safe_float(hist[col].iloc[-1])
            if value is not None:
                return value
    return None


def _normalize_symbol(symbol: str) -> str:
    return normalize_symbol(symbol)


def _is_alpha_vantage_error(payload) -> bool:
    return _is_alpha_vantage_error_impl(payload)


def _has_financial_reports(payload) -> bool:
    return _has_financial_reports_impl(payload)


def _normalize_line_name(name) -> str:
    return _normalize_line_name_impl(name)


def _month_to_quarter(month_str):
    return _month_to_quarter_impl(month_str)


def _safe_decimal(value):
    return _safe_decimal_impl(value)


def _decimal_to_string(value):
    return _decimal_to_string_impl(value)


def _compute_annual_from_quarters(quarterly_reports):
    return _compute_annual_from_quarters_impl(quarterly_reports)


def _compute_partial_year_reports(quarterly_reports):
    return _compute_partial_year_reports_impl(quarterly_reports)
