import pandas as pd
import pytest

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


def test_get_summary_includes_range_aware_avg_consecutive_touch_days(monkeypatch):
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-10-01", "2024-10-02", "2024-12-31"]),
            "open": [100.0, 101.0, 102.0],
            "high": [106.0, 107.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [105.0, 106.0, 102.0],
            "BB_upper": [105.0, 105.0, 110.0],
            "BB_lower": [95.0, 95.0, 90.0],
        }
    )

    monkeypatch.setattr(summary_core, "_load_summary_frame", lambda symbol: df)
    payload = summary_core.get_summary("TEST")

    avg_consecutive = payload["avg_consecutive_touch_days"]
    assert set(avg_consecutive.keys()) == {"1M", "3M", "YTD", "1Y"}
    for range_key in ("1M", "3M", "YTD", "1Y"):
        assert set(avg_consecutive[range_key].keys()) == {"upper", "lower"}

    assert avg_consecutive["1M"]["upper"] is None
    assert avg_consecutive["1M"]["lower"] is None
    assert avg_consecutive["3M"]["upper"] == pytest.approx(2.0)
    assert avg_consecutive["YTD"]["upper"] == pytest.approx(2.0)
    assert avg_consecutive["1Y"]["upper"] == pytest.approx(2.0)
    assert all(avg_consecutive[key]["lower"] is None for key in ("3M", "YTD", "1Y"))
