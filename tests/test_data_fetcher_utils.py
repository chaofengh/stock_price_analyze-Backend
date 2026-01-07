# tests/test_data_fetcher_utils.py

from decimal import Decimal

import pytest

from analysis.data_fetcher_utils import (
    _compute_annual_from_quarters,
    _compute_partial_year_reports,
    _decimal_to_string,
    _has_financial_reports,
    _is_alpha_vantage_error,
    _month_to_quarter,
    _normalize_line_name,
    _normalize_symbol,
    _safe_decimal,
)


def test_normalize_symbol():
    assert _normalize_symbol(" aapl ") == "AAPL"
    assert _normalize_symbol("") == ""
    assert _normalize_symbol(None) == ""


def test_is_alpha_vantage_error():
    assert _is_alpha_vantage_error(None) is True
    assert _is_alpha_vantage_error({"Note": "limit"}) is True
    assert _is_alpha_vantage_error({"Error Message": "bad"}) is True
    assert _is_alpha_vantage_error({"Information": "limit"}) is True
    assert _is_alpha_vantage_error({"ok": True}) is False


def test_has_financial_reports():
    assert _has_financial_reports({"annualReports": [{"a": 1}]}) is True
    assert _has_financial_reports({"quarterlyReports": [{"a": 1}]}) is True
    assert _has_financial_reports({"partialYearReports": [{"a": 1}]}) is True
    assert _has_financial_reports({"annualReports": [], "quarterlyReports": []}) is False
    assert _has_financial_reports("bad") is False


def test_normalize_line_name():
    assert _normalize_line_name("Operating Income") == "operatingincome"
    assert _normalize_line_name("Total-Revenue!") == "totalrevenue"
    assert _normalize_line_name(None) == ""


@pytest.mark.parametrize(
    "month, expected",
    [
        ("1", 1),
        ("3", 1),
        ("4", 2),
        ("6", 2),
        ("7", 3),
        ("9", 3),
        ("10", 4),
        ("12", 4),
        ("13", None),
        ("bad", None),
    ],
)
def test_month_to_quarter(month, expected):
    assert _month_to_quarter(month) == expected


def test_safe_decimal_and_decimal_to_string():
    assert _safe_decimal(None) == Decimal("0")
    assert _safe_decimal("") == Decimal("0")
    assert _safe_decimal("None") == Decimal("0")
    assert _safe_decimal("10.5") == Decimal("10.5")
    assert _safe_decimal("bad") is None
    assert _decimal_to_string(Decimal("10")) == "10"
    assert _decimal_to_string(Decimal("10.50")) == "10.50"


def test_compute_annual_from_quarters():
    reports = [
        {"fiscalDateEnding": "2023-03-31", "totalRevenue": "100", "reportedCurrency": "USD"},
        {"fiscalDateEnding": "2023-06-30", "totalRevenue": "110", "reportedCurrency": "USD"},
        {"fiscalDateEnding": "2023-09-30", "totalRevenue": "120", "reportedCurrency": "USD"},
        {"fiscalDateEnding": "2023-12-31", "totalRevenue": "130", "reportedCurrency": "USD"},
        {"fiscalDateEnding": "2022-12-31", "totalRevenue": "90", "reportedCurrency": "USD"},
    ]
    annual = _compute_annual_from_quarters(reports)
    assert len(annual) == 1
    assert annual[0]["fiscalDateEnding"] == "2023-12-31"
    assert annual[0]["totalRevenue"] == "460"
    assert annual[0]["reportedCurrency"] == "USD"


def test_compute_partial_year_reports():
    reports = [
        {"fiscalDateEnding": "2024-06-30", "totalRevenue": "200"},
        {"fiscalDateEnding": "2024-03-31", "totalRevenue": "180"},
        {"fiscalDateEnding": "2023-06-30", "totalRevenue": "190"},
        {"fiscalDateEnding": "2023-03-31", "totalRevenue": "170"},
        {"fiscalDateEnding": "2022-06-30", "totalRevenue": "160"},
        {"fiscalDateEnding": "2022-03-31", "totalRevenue": "150"},
    ]
    partial = _compute_partial_year_reports(reports)
    assert len(partial) == 3
    assert partial[0]["year"] == "2022"
    assert partial[-1]["year"] == "2024"
    for report in partial:
        assert report["quarterRange"] == "Q1-Q2"
        assert report["quarterCount"] == 2
        assert report["quartersIncluded"] == ["Q1", "Q2"]
