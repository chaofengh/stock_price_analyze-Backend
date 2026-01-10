import pandas as pd

from analysis.chart_builder import build_chart_data


def test_build_chart_data_handles_non_contiguous_index():
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
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
    assert chart[2]["close"] == 102.0
