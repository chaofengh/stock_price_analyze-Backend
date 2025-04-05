#test_opening_range_breakout.py
import datetime
import pandas as pd
import pytz
import numpy as np
import pytest
from unittest.mock import patch, MagicMock

# Import the functions you want to test
from analysis.opening_range_breakout import (
    is_us_eastern_dst,
    fetch_intraday_data,
    compute_metrics,
    backtest_opening_range_breakout,
    run_opening_range_breakout_tests
)


@pytest.mark.parametrize("test_date,expected_dst", [
    (datetime.date(2023, 6, 15), True),   # A date in June (DST)
    (datetime.date(2023, 1, 15), False),    # A date in January (no DST)
])
def test_is_us_eastern_dst(test_date, expected_dst):
    result = is_us_eastern_dst(test_date)
    assert result == expected_dst, f"For date {test_date}, expected {expected_dst} but got {result}"


@patch("analysis.opening_range_breakout.yf.download")
def test_fetch_intraday_data(mock_download):
    # Create a mock DataFrame to return
    mock_data = {
        "Open": [100, 101, 102],
        "High": [101, 102, 103],
        "Low": [99, 100, 101],
        "Close": [100.5, 101.5, 102.5],
        "Volume": [1000, 1200, 1100]
    }
    index = pd.date_range("2023-01-15 14:30:00", periods=3, freq="5T")
    df_mock = pd.DataFrame(mock_data, index=index)
    mock_download.return_value = df_mock

    result = fetch_intraday_data("TSLA", days=5, interval="5m")

    mock_download.assert_called_once_with(
        "TSLA",
        period="5d",
        interval="5m",
        auto_adjust=True,
        progress=False
    )
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert isinstance(result.index, pd.DatetimeIndex)
    assert str(result.index.tz) == "UTC", "Index should be converted to UTC"
    assert "timestamp_utc" in result.columns, "Expected 'timestamp_utc' column in the result"


def test_compute_metrics():
    # Example trade PnLs: 2 wins, 2 losses
    trades = pd.DataFrame({"pnl": [100, -50, 200, -100]})
    metrics = compute_metrics(trades)

    assert "win_rate" in metrics
    assert "profit_factor" in metrics
    assert "sharpe_ratio" in metrics
    assert "max_drawdown" in metrics
    assert "num_trades" in metrics

    assert metrics["num_trades"] == 4
    assert metrics["win_rate"] == 0.5
    assert metrics["profit_factor"] == 2
    # For this simple series, max_drawdown should be -100
    assert metrics["max_drawdown"] == -100


def test_backtest_opening_range_breakout():
    # Create a DataFrame for a single DST day (June)
    date_str = "2023-06-15"
    times = pd.date_range(f"{date_str} 13:30", periods=10, freq="5T", tz="UTC")
    # Construct data so that first 6 bars form an opening range and 7th bar triggers a breakout (long)
    data = {
        "Open": [100] * 10,
        "High": [100, 101, 102, 101, 100, 100, 103, 103, 103, 103],
        "Low":  [99] * 10,
        "Close": [100] * 6 + [103] * 4,
        "Volume": [1000] * 10
    }
    df_test = pd.DataFrame(data, index=times)

    trades = backtest_opening_range_breakout(df_test, open_range_minutes=30)

    assert isinstance(trades, pd.DataFrame)
    if not trades.empty:
        assert "direction" in trades.columns
        assert "pnl" in trades.columns
        assert trades.iloc[0]["direction"] == "long"
    else:
        pytest.fail("Expected at least one trade, but got none.")


@patch("analysis.opening_range_breakout.fetch_intraday_data")
def test_run_opening_range_breakout_tests(mock_fetch):
    # Create a DataFrame with enough rows to produce a trade
    date_str = "2023-06-15"
    times = pd.date_range(f"{date_str} 13:30", periods=10, freq="5T", tz="UTC")
    data = {
        "Open": [100] * 10,
        # First 6 bars define opening range with max = 102, then breakout in 7th bar with High = 103
        "High": [100, 101, 102, 101, 100, 100, 103, 103, 103, 103],
        "Low":  [99] * 10,
        "Close": [100] * 6 + [103] * 4,
        "Volume": [1000] * 10
    }
    df_mock = pd.DataFrame(data, index=times)
    mock_fetch.return_value = df_mock

    result = run_opening_range_breakout_tests("TSLA", days=1, interval="5m")

    # Check structure of result
    assert "scenarios" in result
    assert "intraday_data" in result
    assert isinstance(result["scenarios"], list)
    assert isinstance(result["intraday_data"], list)

    # Check that each scenario has expected fields if scenarios are generated
    for scenario in result["scenarios"]:
        for field in (
            "scenario_name", "filters", "open_range_minutes",
            "win_rate", "profit_factor", "sharpe_ratio", "max_drawdown",
            "num_trades", "net_pnl"
        ):
            assert field in scenario

    # If intraday_data is available, check for renamed columns
    if result["intraday_data"]:
        sample_row = result["intraday_data"][0]
        for col in ("open", "high", "low", "close", "volume", "date"):
            assert col in sample_row
