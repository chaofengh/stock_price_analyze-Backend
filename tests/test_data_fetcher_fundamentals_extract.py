# tests/test_data_fetcher_fundamentals_extract.py

import pandas as pd
import pytest

from analysis.data_fetcher_fundamentals_extract import extract_fundamentals, is_empty_fundamentals


def _make_statement_df(values, col="2023-12-31"):
    return pd.DataFrame({col: pd.Series(values)})


def test_extract_fundamentals_derives_metrics_from_info_and_statements():
    info = {
        "trailingEps": 5.0,
        "forwardEps": 6.0,
        "earningsGrowth": 0.1,
        "currentPrice": 120.0,
        "sharesOutstanding": 1_000_000_000,
        "fullTimeEmployees": 10,
    }
    fast_info = {}
    income = _make_statement_df(
        {
            "Total Revenue": 1000,
            "Cost Of Revenue": 400,
            "Operating Income": 200,
            "Income Before Tax": 180,
            "Income Tax Expense": 30,
            "Net Income": 150,
        }
    )
    balance = _make_statement_df(
        {
            "Total Assets": 2000,
            "Total Current Liabilities": 500,
            "Total Stockholder Equity": 800,
            "Total Debt": 300,
            "Cash And Cash Equivalents": 100,
        }
    )
    cashflow = _make_statement_df(
        {
            "Capital Expenditures": -50,
            "Operating Cash Flow": 250,
        }
    )

    result = extract_fundamentals(
        info,
        fast_info,
        statements={
            "income": income,
            "balance": balance,
            "cashflow": cashflow,
        },
    )

    assert result["trailingPE"] == pytest.approx(24.0)
    assert result["forwardPE"] == pytest.approx(20.0)
    assert result["PEG"] == pytest.approx(2.0)
    assert result["PGI"] == pytest.approx(20.0 / 24.0)
    assert result["marketCap"] == pytest.approx(120.0 * 1_000_000_000)
    assert result["grossMargin"] == pytest.approx(0.6)
    assert result["freeCashFlowMargin"] == pytest.approx(0.2)
    assert result["metricTrends"] is not None


def test_extract_fundamentals_uses_alpha_quarterly_for_roic():
    alpha_financials = {
        "income_statement": {
            "quarterlyReports": [
                {
                    "fiscalDateEnding": "2024-06-30",
                    "totalRevenue": "1000",
                    "costOfRevenue": "400",
                    "operatingIncome": "200",
                    "incomeBeforeTax": "250",
                    "incomeTaxExpense": "50",
                },
                {
                    "fiscalDateEnding": "2024-03-31",
                    "totalRevenue": "900",
                    "costOfRevenue": "360",
                    "operatingIncome": "180",
                    "incomeBeforeTax": "225",
                    "incomeTaxExpense": "45",
                },
            ]
        },
        "balance_sheet": {
            "quarterlyReports": [
                {
                    "fiscalDateEnding": "2024-06-30",
                    "totalAssets": "2000",
                    "totalCurrentLiabilities": "500",
                    "totalStockholderEquity": "700",
                    "totalDebt": "300",
                    "cashAndCashEquivalentsAtCarryingValue": "100",
                },
                {
                    "fiscalDateEnding": "2024-03-31",
                    "totalAssets": "1950",
                    "totalCurrentLiabilities": "480",
                    "totalStockholderEquity": "680",
                    "totalDebt": "290",
                    "cashAndCashEquivalentsAtCarryingValue": "95",
                },
            ]
        },
        "cash_flow": {
            "quarterlyReports": [
                {
                    "fiscalDateEnding": "2024-06-30",
                    "operatingCashflow": "250",
                    "capitalExpenditures": "-50",
                },
                {
                    "fiscalDateEnding": "2024-03-31",
                    "operatingCashflow": "230",
                    "capitalExpenditures": "-45",
                },
            ]
        },
    }
    result = extract_fundamentals(
        info={"fullTimeEmployees": 10},
        fast_info={},
        statements={"alpha_financials": alpha_financials},
    )
    assert result["roic"] == pytest.approx(640 / 900)
    assert result["metricTrends"]["roic"]["recent"] != []


def test_is_empty_fundamentals():
    assert is_empty_fundamentals({}) is True
    assert is_empty_fundamentals({"trailingPE": None, "beta": None}) is True
    assert is_empty_fundamentals({"trailingPE": 10.0}) is False


def test_extract_fundamentals_peg_falls_back_to_trailing_peg():
    info = {
        "trailingPegRatio": 1.2,
        "forwardPE": None,
        "earningsGrowth": None,
    }
    result = extract_fundamentals(info, {}, statements={})
    assert result["PEG"] == pytest.approx(1.2)


def test_extract_fundamentals_pgi_falls_back_to_eps():
    info = {
        "trailingEps": 4.0,
        "forwardEps": 5.0,
    }
    result = extract_fundamentals(info, {}, statements={})
    assert result["PGI"] == pytest.approx(0.8)
