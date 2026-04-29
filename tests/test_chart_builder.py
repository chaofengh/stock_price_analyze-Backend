import pandas as pd

from analysis.chart_builder import build_chart_data


def test_build_chart_data_handles_non_contiguous_index():
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
            "open": [99.0, 100.5, 101.0],
            "high": [101.0, 102.0, 103.0],
            "low": [98.5, 100.0, 100.5],
            "close": [100.0, 101.5, 102.0],
            "BB_upper": [105.0, 106.0, 107.0],
            "BB_lower": [95.0, 96.0, 97.0],
        },
        index=[10, 12, 15],
    )

    touches = [{"index": 1}]
    chart = build_chart_data(df, touches)

    assert len(chart) == 3
    assert chart[1]["isTouch"] is True
    assert chart[0]["date"] == "2024-01-02"
    assert chart[0]["open"] == 99.0
    assert chart[1]["high"] == 102.0
    assert chart[2]["low"] == 100.5
    assert chart[2]["close"] == 102.0


def test_build_chart_data_converts_missing_ohlc_and_bands_to_null():
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02"]),
            "open": [None],
            "high": [float("nan")],
            "low": [98.5],
            "close": [float("nan")],
            "BB_upper": [None],
            "BB_lower": [float("nan")],
        }
    )

    chart = build_chart_data(df, touches=[])

    assert chart[0]["open"] is None
    assert chart[0]["high"] is None
    assert chart[0]["low"] == 98.5
    assert chart[0]["close"] is None
    assert chart[0]["upper"] is None
    assert chart[0]["lower"] is None
