from datetime import date, datetime

import pandas as pd
import yfinance as yf

from .financials_helpers import safe_decimal, decimal_to_string


def normalize_line_name(name) -> str:
    if name is None:
        return ""
    return "".join(ch for ch in str(name).lower() if ch.isalnum())


_YF_INCOME_MAP = {
    "totalrevenue": "totalRevenue",
    "grossprofit": "grossProfit",
    "ebitda": "ebitda",
    "operatingincome": "operatingIncome",
    "operatingincomeloss": "operatingIncome",
    "operatingexpense": "operatingExpenses",
    "operatingexpenses": "operatingExpenses",
    "researchdevelopment": "researchAndDevelopment",
    "researchanddevelopment": "researchAndDevelopment",
    "researchdevelopmentexpense": "researchAndDevelopment",
    "researchanddevelopmentexpense": "researchAndDevelopment",
    "netincome": "netIncome",
    "netincomeloss": "netIncome",
}


def _parse_info_date(value):
    if value is None:
        return None
    if hasattr(value, "to_pydatetime"):
        try:
            value = value.to_pydatetime()
        except Exception:
            return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)):
        try:
            timestamp = float(value)
        except (TypeError, ValueError):
            return None
        if timestamp > 1e12:
            timestamp = timestamp / 1000
        try:
            return datetime.utcfromtimestamp(timestamp).date()
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return datetime.strptime(raw[:10], "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


def _quarter_label_from_dates(most_recent, last_fiscal):
    if not most_recent:
        return None
    if not last_fiscal:
        quarter = (most_recent.month - 1) // 3 + 1
        return f"Q{quarter}"
    fiscal_end_month = last_fiscal.month
    fiscal_start_month = (fiscal_end_month % 12) + 1
    offset = (most_recent.month - fiscal_start_month + 12) % 12
    quarter = offset // 3 + 1
    return f"Q{quarter}"


def get_fiscal_quarter_info(symbol: str) -> dict:
    try:
        ticker = yf.Ticker(symbol)
    except Exception:
        return {}

    info = {}
    try:
        if hasattr(ticker, "get_info"):
            info = ticker.get_info() or {}
    except Exception:
        info = {}
    if not info:
        try:
            info = ticker.info or {}
        except Exception:
            info = {}

    if not isinstance(info, dict) or not info:
        return {}

    most_recent = _parse_info_date(info.get("mostRecentQuarter"))
    last_fiscal = _parse_info_date(info.get("lastFiscalYearEnd"))
    if not most_recent and not last_fiscal:
        return {}

    label = _quarter_label_from_dates(most_recent, last_fiscal)
    payload = {}
    if most_recent:
        payload["mostRecentQuarter"] = most_recent.isoformat()
    if last_fiscal:
        payload["lastFiscalYearEnd"] = last_fiscal.isoformat()
    if label:
        payload["mostRecentQuarterLabel"] = label
    return payload


def build_income_annual_from_yfinance(symbol: str) -> list:
    try:
        ticker = yf.Ticker(symbol)
        financials = ticker.financials
    except Exception:
        return []

    if financials is None or getattr(financials, "empty", True):
        return []

    reports = []
    for col in financials.columns:
        if hasattr(col, "strftime"):
            fiscal_date = col.strftime("%Y-%m-%d")
        else:
            fiscal_date = str(col)

        report = {"fiscalDateEnding": fiscal_date}
        for idx, value in financials[col].items():
            key = _YF_INCOME_MAP.get(normalize_line_name(idx))
            if not key:
                continue
            if value is None or pd.isna(value):
                continue
            numeric_value = safe_decimal(value)
            if numeric_value is None:
                continue
            report[key] = decimal_to_string(numeric_value)

        if len(report) > 1:
            reports.append(report)

    return sorted(reports, key=lambda r: r.get("fiscalDateEnding", ""), reverse=True)
