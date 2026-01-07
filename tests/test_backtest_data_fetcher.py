# tests/test_backtest_data_fetcher.py

from unittest.mock import Mock

import pandas as pd

from backtest_strategies.data_fetcher import fetch_intraday_data


def test_fetch_intraday_data_sets_index_and_sorts(monkeypatch):
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-03", "2024-01-02"]),
            "open": [100, 99],
            "high": [101, 100],
            "low": [98, 97],
            "close": [100.5, 99.5],
            "volume": [1000, 900],
        }
    )
    mock_fetch = Mock(return_value={"AAPL": df})
    monkeypatch.setattr("backtest_strategies.data_fetcher.fetch_stock_data", mock_fetch)

    result = fetch_intraday_data("AAPL")
    assert isinstance(result.index, pd.DatetimeIndex)
    assert result.index[0] < result.index[1]


def test_fetch_intraday_data_returns_empty_on_missing(monkeypatch):
    mock_fetch = Mock(return_value={})
    monkeypatch.setattr("backtest_strategies.data_fetcher.fetch_stock_data", mock_fetch)
    result = fetch_intraday_data("AAPL")
    assert result.empty
