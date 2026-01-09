# tests/test_summary_peer_utils.py

import pandas as pd
import pytest
from analysis import summary_peer_utils


def test_get_peer_info_builds_prices(monkeypatch):
    df = pd.DataFrame({"close": [10.0, 11.0, 12.0]})

    def fake_fetch(symbols, period, interval, require_ohlc, threads):
        assert symbols == ["AAA", "BBB"]
        assert period == "5d"
        assert interval == "5m"
        assert require_ohlc is False
        assert threads is False
        return {"AAA": df, "BBB": df}

    monkeypatch.setattr(summary_peer_utils, "fetch_stock_data", fake_fetch)

    peer_info = summary_peer_utils.get_peer_info(["AAA", "BBB"])

    assert peer_info["AAA"]["latest_price"] == 12.0
    assert peer_info["AAA"]["percentage_change"] == pytest.approx(20.0)
    assert len(peer_info["AAA"]["intraday_close_5m"]) == 3


def test_get_peer_info_handles_zero_first_close(monkeypatch):
    df = pd.DataFrame({"close": [0.0, 5.0]})

    def fake_fetch(symbols, period, interval, require_ohlc, threads):
        return {"AAA": df}

    monkeypatch.setattr(summary_peer_utils, "fetch_stock_data", fake_fetch)

    peer_info = summary_peer_utils.get_peer_info(["AAA"])

    assert peer_info["AAA"]["latest_price"] == 5.0
    assert peer_info["AAA"]["percentage_change"] is None


def test_get_peer_info_includes_missing_peer(monkeypatch):
    df = pd.DataFrame({"close": [1.0, 2.0]})

    def fake_fetch(symbols, period, interval, require_ohlc, threads):
        return {"AAA": df}

    monkeypatch.setattr(summary_peer_utils, "fetch_stock_data", fake_fetch)

    peer_info = summary_peer_utils.get_peer_info(["AAA", "MISSING"])

    assert peer_info["AAA"]["latest_price"] == 2.0
    assert peer_info["MISSING"]["latest_price"] is None
    assert peer_info["MISSING"]["intraday_close_5m"] == []


def test_get_peer_metric_averages_ignores_missing(monkeypatch):
    def fake_fundamentals(symbol):
        if symbol == "AAA":
            return {"trailingPE": 10.0, "forwardPE": 12.0}
        return {"trailingPE": None, "forwardPE": 14.0}

    monkeypatch.setattr(summary_peer_utils, "get_fundamentals_light", fake_fundamentals)

    averages = summary_peer_utils.get_peer_metric_averages(["AAA", "BBB"])

    assert averages["avg_peer_trailingPE"] == 10.0
    assert averages["avg_peer_forwardPE"] == pytest.approx(13.0)
