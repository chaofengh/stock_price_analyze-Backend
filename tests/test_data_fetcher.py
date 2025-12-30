# tests/test_data_fetcher.py
from unittest.mock import Mock, patch
import pandas as pd
import pytest
from analysis.data_fetcher import fetch_stock_data, fetch_peers, fetch_stock_fundamentals

@patch("analysis.data_fetcher_market.yf.download")
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

@patch("analysis.data_fetcher_fundamentals.finnhub_client.company_peers", return_value=["AAPL", "GOOGL"])
def test_fetch_peers_success(mock_peers):
    peers = fetch_peers("MSFT")
    assert peers == ["AAPL", "GOOGL"]
    mock_peers.assert_called_once_with("MSFT")


@patch("analysis.data_fetcher_fundamentals.yf.Ticker")
def test_fetch_stock_fundamentals_computes_ratios(mock_ticker):
    ticker = Mock()
    ticker.get_info.return_value = {
        "trailingEps": 5.0,
        "forwardEps": 6.0,
        "earningsGrowth": 0.2,
        "regularMarketPrice": 200.0,
    }
    ticker.get_fast_info.return_value = {}
    ticker.history.return_value = pd.DataFrame()
    mock_ticker.return_value = ticker

    result = fetch_stock_fundamentals("AAPL")
    assert result["trailingPE"] == pytest.approx(40.0)
    assert result["forwardPE"] == pytest.approx(200.0 / 6.0)
    assert result["PEG"] == pytest.approx((200.0 / 6.0) / 20.0)
    assert result["PGI"] == pytest.approx((200.0 / 6.0) / 40.0)


@patch("analysis.data_fetcher_fundamentals.yf.Ticker")
def test_fetch_stock_fundamentals_peg_fallback(mock_ticker):
    ticker = Mock()
    ticker.get_info.return_value = {
        "forwardPE": 25.0,
        "trailingPegRatio": 1.3,
    }
    ticker.get_fast_info.return_value = {}
    ticker.history.return_value = pd.DataFrame()
    mock_ticker.return_value = ticker

    result = fetch_stock_fundamentals("AAPL")
    assert result["PEG"] == pytest.approx(1.3)


@patch("analysis.data_fetcher_fundamentals.yf.Ticker")
def test_fetch_stock_fundamentals_pgi_from_eps(mock_ticker):
    ticker = Mock()
    ticker.get_info.return_value = {
        "trailingEps": 4.0,
        "forwardEps": 5.0,
    }
    ticker.get_fast_info.return_value = {}
    ticker.history.return_value = pd.DataFrame()
    mock_ticker.return_value = ticker

    result = fetch_stock_fundamentals("AAPL")
    assert result["PGI"] == pytest.approx(0.8)
