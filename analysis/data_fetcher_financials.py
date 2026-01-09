"""
data_fetcher_financials.py
Purpose: fetch financial statements from Alpha Vantage or the database.
"""
import os

import requests
from dotenv import load_dotenv

from database.financials_repository import get_financial_statement, upsert_financial_statement
from .data_fetcher_utils import normalize_symbol
from .financials_alpha import is_alpha_vantage_error, has_financial_reports
from .financials_helpers import compute_annual_from_quarters, compute_partial_year_reports

load_dotenv()

alpha_vantage_api_key = os.environ.get("alpha_vantage_api_key")


def _alpha_error_message(payload: dict) -> str:
    if not isinstance(payload, dict):
        return "Alpha Vantage returned an invalid payload."
    return (
        payload.get("Note")
        or payload.get("Information")
        or payload.get("Error Message")
        or "Alpha Vantage returned an error payload."
    )


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
        db_payload = None
        try:
            db_payload = get_financial_statement(symbol, statement)
        except Exception:
            db_payload = None

        if isinstance(db_payload, dict):
            if db_payload.get("symbol") is None:
                db_payload["symbol"] = symbol
            if has_financial_reports(db_payload):
                financials[statement] = db_payload
                continue

        if not alpha_vantage_api_key:
            raise ValueError("Missing 'alpha_vantage_api_key' in environment")

        function_name = valid_types[statement]
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

        if has_financial_reports(data):
            try:
                upsert_financial_statement(symbol, statement, data, source="alpha_vantage")
            except Exception:
                pass

        financials[statement] = data

    return financials
