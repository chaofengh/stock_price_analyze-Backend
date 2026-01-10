import pandas as pd

from analysis import summary_core


def test_get_summary_handles_non_contiguous_index(monkeypatch):
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
            "open": [100.0, 101.0, 102.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.0, 101.0, 102.0],
            "BB_upper": [105.0, 106.0, 107.0],
            "BB_lower": [95.0, 96.0, 97.0],
        },
        index=[5, 7, 9],
    )

    monkeypatch.setattr(summary_core, "_load_summary_frame", lambda symbol: df)
    payload = summary_core.get_summary("TEST")

    assert payload["symbol"] == "TEST"
    assert payload["final_price"] == 102.0
    assert payload["price_change_in_dollars"] == 2.0
    assert len(payload["chart_data"]) == 3
