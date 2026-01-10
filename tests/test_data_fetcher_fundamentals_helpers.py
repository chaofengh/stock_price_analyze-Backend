# tests/test_data_fetcher_fundamentals_helpers.py

import pandas as pd
import pytest

from analysis.data_fetcher_fundamentals_helpers import (
    alpha_dates,
    alpha_period_key,
    alpha_report_lookup,
    alpha_report_map,
    alpha_report_map_by_period,
    latest_series_value,
    safe_div,
    safe_float,
    split_trends,
    statement_columns_by_recency,
    statement_float,
    statement_value_at,
)


def test_safe_float_and_div():
    assert safe_float(None) is None
    assert safe_float("bad") is None
    assert safe_float(float("nan")) is None
    assert safe_float(float("inf")) is None
    assert safe_float("10.5") == 10.5
    assert safe_float("12,345") == 12345.0
    assert safe_div(None, 2) is None
    assert safe_div(1, 0) is None
    assert safe_div(10, 2) == 5


def test_statement_columns_by_recency():
    df = pd.DataFrame(
        {"2023-12-31": [1], "2022-12-31": [2]},
        index=["Total Revenue"],
    )
    cols = statement_columns_by_recency(df)
    assert cols[0] == "2023-12-31"

    df_bad = pd.DataFrame({"A": [1], "B": [2]}, index=["Total Revenue"])
    cols_bad = statement_columns_by_recency(df_bad)
    assert cols_bad == ["A", "B"]


def test_statement_float_and_value_at():
    df = pd.DataFrame(
        {"2023-12-31": [100.0], "2022-12-31": [90.0]},
        index=["Total Revenue"],
    )
    assert statement_float(df, ("Total Revenue",)) == 100.0
    assert statement_value_at(df, ("Total Revenue",), "2022-12-31") == 90.0
    assert statement_value_at(df, ("Total Revenue",), "missing") is None


def test_alpha_report_maps_and_lookup():
    reports = [
        {"fiscalDateEnding": "2024-03-31", "value": "10"},
        {"fiscalDateEnding": "2023-12-31", "value": "9"},
    ]
    date_map = alpha_report_map(reports)
    assert date_map["2024-03-31"]["value"] == "10"
    assert alpha_period_key("2024-03-31") == ("2024", 1)
    period_map = alpha_report_map_by_period(reports)
    assert ("2024", 1) in period_map
    lookup = alpha_report_lookup(period_map, date_map, "2024-03-31")
    assert lookup["value"] == "10"
    assert alpha_dates(reports) == ["2024-03-31", "2023-12-31"]


def test_split_trends_and_latest_series_value():
    series = [1, 2, 3, 4, 5, 6, 7, 8]
    trends = split_trends(series)
    assert trends["recent"] == [4, 3, 2, 1]
    assert trends["prior"] == [8, 7, 6, 5]
    assert latest_series_value([None, None, 3, 2]) == 3
    assert latest_series_value([]) is None
