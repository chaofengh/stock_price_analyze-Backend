# tests/test_data_fetcher_fundamentals_metrics.py

import pandas as pd
import pytest

from analysis.data_fetcher_fundamentals_metrics import (
    build_metric_snapshot,
    build_metric_snapshot_av,
)


def _make_statement_df(values, col="2023-12-31"):
    return pd.DataFrame({col: pd.Series(values)})


def test_build_metric_snapshot_computes_ratios():
    income_df = _make_statement_df(
        {
            "Total Revenue": 1000,
            "Cost Of Revenue": 400,
            "Operating Income": 200,
            "EBITDA": 220,
            "Selling General Administrative": 100,
            "Research Development": 50,
            "Income Before Tax": 180,
            "Income Tax Expense": 30,
            "Net Income": 150,
        }
    )
    balance_df = _make_statement_df(
        {
            "Total Assets": 2000,
            "Total Current Liabilities": 500,
            "Total Stockholder Equity": 800,
            "Total Debt": 300,
            "Cash And Cash Equivalents": 100,
        }
    )
    cashflow_df = _make_statement_df(
        {
            "Capital Expenditures": -50,
            "Operating Cash Flow": 250,
        }
    )

    snapshot = build_metric_snapshot(
        [
            "revenuePerEmployee",
            "grossProfitPerEmployee",
            "roic",
            "freeCashFlowMargin",
            "grossMargin",
        ],
        income_df,
        balance_df,
        cashflow_df,
        "2023-12-31",
        tax_rate_info=0.2,
        employees=10,
    )

    assert snapshot["revenuePerEmployee"] == 100.0
    assert snapshot["grossProfitPerEmployee"] == 60.0
    assert snapshot["freeCashFlowMargin"] == pytest.approx(0.2)
    assert snapshot["grossMargin"] == pytest.approx(0.6)
    assert snapshot["roic"] == pytest.approx(1 / 6)


def test_build_metric_snapshot_av_computes_ratios():
    income_report = {
        "totalRevenue": "1000",
        "costOfRevenue": "400",
        "operatingIncome": "200",
        "ebitda": "220",
        "sellingGeneralAdministrative": "100",
        "researchAndDevelopment": "50",
        "incomeBeforeTax": "180",
        "incomeTaxExpense": "30",
        "netIncome": "150",
    }
    balance_report = {
        "totalAssets": "2000",
        "totalCurrentLiabilities": "500",
        "totalShareholderEquity": "800",
        "shortLongTermDebtTotal": "300",
        "cashAndCashEquivalentsAtCarryingValue": "100",
    }
    cashflow_report = {"capitalExpenditures": "-50", "operatingCashflow": "250"}

    snapshot = build_metric_snapshot_av(
        [
            "revenuePerEmployee",
            "grossProfitPerEmployee",
            "roic",
            "freeCashFlowMargin",
            "grossMargin",
        ],
        income_report,
        balance_report,
        cashflow_report,
        tax_rate_info=0.2,
        employees=10,
    )

    assert snapshot["revenuePerEmployee"] == 100.0
    assert snapshot["grossProfitPerEmployee"] == 60.0
    assert snapshot["freeCashFlowMargin"] == pytest.approx(0.2)
    assert snapshot["grossMargin"] == pytest.approx(0.6)
    assert snapshot["roic"] == pytest.approx(1 / 6)


def test_build_metric_snapshot_handles_missing_column():
    assert build_metric_snapshot([], None, None, None, None, None, None) == {}
