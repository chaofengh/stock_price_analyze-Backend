# tests/test_data_fetcher.py
from unittest.mock import patch
import pandas as pd
import numpy as np
import pytest
from analysis.data_fetcher import fetch_stock_data, fetch_peers

@patch("analysis.data_fetcher.yf.download")
def test_fetch_stock_data_single_ticker(mock_download):
    # Setup the fake DataFrame that yfinance would return
    fake_df = pd.DataFrame({
        "Open": [100, 101],
        "High": [105, 106],
        "Low": [99, 100],
        "Close": [104, 105],
        "Adj Close": [104, 105],
        "Volume": [1000, 1100],
    }, index=pd.to_datetime(["2023-01-01", "2023-01-02"]))
    # yfinance can return a multi-index if multiple tickers are passed in,
    # so make sure your test shape matches what your code expects.
    mock_download.return_value = {"TSLA": fake_df}  # or a more complicated structure

    result = fetch_stock_data("TSLA", period="1d", interval="1m")
    assert "TSLA" in result
    df = result["TSLA"]
    # Basic checks
    assert len(df) == 2
    assert all(col in df.columns for col in ["date", "open", "high", "low", "close", "volume"])

@patch("analysis.data_fetcher.finnhub_client.company_peers", return_value=["AAPL", "GOOGL"])
def test_fetch_peers_success(mock_peers):
    peers = fetch_peers("MSFT")
    assert peers == ["AAPL", "GOOGL"]
    mock_peers.assert_called_once_with("MSFT")
