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
