"""
data_fetcher_financials.py
Purpose: fetch financial statements from Alpha Vantage or the database.
"""
import os
from datetime import date, datetime, timedelta

import requests
from dotenv import load_dotenv

from database.financials_repository import get_financial_statement, upsert_financial_statement
from .data_fetcher_utils import normalize_symbol
from .financials_alpha import is_alpha_vantage_error, has_financial_reports
from .financials_helpers import compute_annual_from_quarters, compute_partial_year_reports
from .financials_yfinance import get_fiscal_quarter_info

load_dotenv()

alpha_vantage_api_key = os.environ.get("alpha_vantage_api_key")
_DEFAULT_MAX_QUARTERLY_AGE_DAYS = 90
_DEFAULT_MAX_ANNUAL_AGE_DAYS = 370


def _alpha_error_message(payload: dict) -> str:
    if not isinstance(payload, dict):
        return "Alpha Vantage returned an invalid payload."
    return (
        payload.get("Note")
        or payload.get("Information")
        or payload.get("Error Message")
        or "Alpha Vantage returned an error payload."
    )


def _parse_max_age_days(env_key: str, default_days: int) -> int:
    raw = os.environ.get(env_key)
    if raw is None:
        return default_days
    try:
        days = int(raw)
    except (TypeError, ValueError):
        return default_days
    return max(days, 0)


def _utc_today() -> date:
    return datetime.utcnow().date()


def _parse_fiscal_date(value: str):
    if not isinstance(value, str):
        return None
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _latest_report_date(payload: dict, key: str):
    if not isinstance(payload, dict):
        return None
    reports = payload.get(key) or []
    if not isinstance(reports, list):
        return None
    dates = []
    for report in reports:
        if not isinstance(report, dict):
            continue
        date_str = report.get("fiscalDateEnding")
        parsed = _parse_fiscal_date(date_str)
        if parsed is not None:
            dates.append(parsed)
    return max(dates) if dates else None


def _attach_quarter_info(payload, quarter_info):
    if not isinstance(payload, dict) or not quarter_info:
        return payload
    for key, value in quarter_info.items():
        if value is None:
            continue
        payload.setdefault(key, value)
    return payload


def _is_fresh_by_reports(payload: dict, max_quarterly_age_days: int, max_annual_age_days: int) -> bool:
    if not isinstance(payload, dict):
        return False
    today = _utc_today()
    quarterly_date = _latest_report_date(payload, "quarterlyReports")
    annual_date = _latest_report_date(payload, "annualReports")

    has_any = quarterly_date is not None or annual_date is not None
    if not has_any:
        return False

    if quarterly_date is not None:
        age = today - quarterly_date
        if age > timedelta(days=max_quarterly_age_days):
            return False

    if annual_date is not None:
        age = today - annual_date
        if age > timedelta(days=max_annual_age_days):
            return False

    return True


def _fetch_alpha_vantage_statement(symbol: str, statement: str, function_name: str) -> dict:
    url = (
        "https://www.alphavantage.co/query"
        f"?function={function_name}&symbol={symbol}&apikey={alpha_vantage_api_key}"
    )
    response = requests.get(url, timeout=8)
    if response.status_code != 200:
        raise Exception(f"Error fetching {statement} data: {response.status_code}")

    data = response.json()
    if not isinstance(data, dict):
        data = {}
    if is_alpha_vantage_error(data):
        raise ValueError(_alpha_error_message(data))

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

    data["symbol"] = symbol
    return data


def fetch_financials(symbol: str, statements=None) -> dict:
    """
    Fetch financial statements (income, balance sheet, cash flow).
    """
    symbol = normalize_symbol(symbol)
    max_quarterly_age_days = _parse_max_age_days(
        "financials_max_quarterly_age_days", _DEFAULT_MAX_QUARTERLY_AGE_DAYS
    )
    max_annual_age_days = _parse_max_age_days(
        "financials_max_annual_age_days", _DEFAULT_MAX_ANNUAL_AGE_DAYS
    )

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

    quarter_info = get_fiscal_quarter_info(symbol)
    financials = {}
    for statement in requested_types:
        db_payload = None
        try:
            db_payload = get_financial_statement(symbol, statement)
        except Exception:
            db_payload = None

        db_has_reports = False
        if isinstance(db_payload, dict):
            if db_payload.get("symbol") is None:
                db_payload["symbol"] = symbol
            db_has_reports = has_financial_reports(db_payload)
            if db_has_reports and _is_fresh_by_reports(
                db_payload, max_quarterly_age_days, max_annual_age_days
            ):
                financials[statement] = _attach_quarter_info(db_payload, quarter_info)
                continue

        if not alpha_vantage_api_key:
            if db_has_reports:
                financials[statement] = _attach_quarter_info(db_payload, quarter_info)
                continue
            raise ValueError("Missing 'alpha_vantage_api_key' in environment")

        function_name = valid_types[statement]
        try:
            data = _fetch_alpha_vantage_statement(symbol, statement, function_name)
        except Exception:
            if db_has_reports:
                financials[statement] = _attach_quarter_info(db_payload, quarter_info)
                continue
            raise

        if has_financial_reports(data):
            api_quarterly = _latest_report_date(data, "quarterlyReports")
            api_annual = _latest_report_date(data, "annualReports")
            db_quarterly = _latest_report_date(db_payload, "quarterlyReports") if db_has_reports else None
            db_annual = _latest_report_date(db_payload, "annualReports") if db_has_reports else None

            should_use_api = not db_has_reports
            if db_has_reports:
                has_newer_quarterly = (
                    api_quarterly is not None
                    and (db_quarterly is None or api_quarterly > db_quarterly)
                )
                has_newer_annual = (
                    api_annual is not None
                    and (db_annual is None or api_annual > db_annual)
                )
                should_use_api = has_newer_quarterly or has_newer_annual
            if should_use_api:
                try:
                    upsert_financial_statement(symbol, statement, data, source="alpha_vantage")
                except Exception:
                    pass
                financials[statement] = _attach_quarter_info(data, quarter_info)
            else:
                financials[statement] = _attach_quarter_info(db_payload, quarter_info)
        else:
            financials[statement] = _attach_quarter_info(db_payload or data, quarter_info)

    return financials
