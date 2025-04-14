import datetime
import pandas as pd
import pytz
import numpy as np
import pytest
from unittest.mock import patch, MagicMock

# Import the functions you want to test
from stock_price_analyze_backend.analysis.backtest_strategies import (
    is_us_eastern_dst,
    fetch_intraday_data,
    compute_metrics,
    backtest_opening_range_breakout,
    run_opening_range_breakout_tests
)


@pytest.mark.parametrize("test_date,expected_dst", [
    (datetime.date(2023, 6, 15), True),   # A date in June (DST)
    (datetime.date(2023, 1, 15), False),  # A date in January (no DST)
])
def test_is_us_eastern_dst(test_date, expected_dst):
    """
    Check that is_us_eastern_dst correctly identifies
    whether a given date is in US/Eastern DST.
    """
    result = is_us_eastern_dst(test_date)
    assert result == expected_dst, (
        f"For date {test_date}, expected {expected_dst} but got {result}"
    )


@patch("analysis.opening_range_breakout.yf.download")
def test_fetch_intraday_data(mock_download):
    """
    Test that fetch_intraday_data:
      1) Calls yfinance.download with the correct parameters.
      2) Returns a DataFrame with a UTC DatetimeIndex and the extra 'timestamp_utc' column.
    """
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

    # Call the function
    result = fetch_intraday_data("TSLA", days=5, interval="5m")

    # Check that yfinance.download was called correctly
    mock_download.assert_called_once_with(
        "TSLA",
        period="5d",
        interval="5m",
        auto_adjust=True,
        progress=False
    )

    # Check that we got a DataFrame with the correct index/timezone
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert isinstance(result.index, pd.DatetimeIndex)
    assert str(result.index.tz) == "UTC", "Index should be converted to UTC"

    # Check the 'timestamp_utc' column
    assert "timestamp_utc" in result.columns, "Expected 'timestamp_utc' column in the result"


def test_compute_metrics():
    """
    Test compute_metrics on a small trades dictionary.
    """
    # Example trade PnLs: 2 wins, 2 losses
    trades = pd.DataFrame({
        "pnl": [100, -50, 200, -100]  # net total: 150
    })
    metrics = compute_metrics(trades)

    # Validate fields
    assert "win_rate" in metrics
    assert "profit_factor" in metrics
    assert "sharpe_ratio" in metrics
    assert "max_drawdown" in metrics
    assert "num_trades" in metrics

    # Basic checks
    assert metrics["num_trades"] == 4
    # There are 2 winning trades (100, 200) and 2 losing trades (-50, -100)
    # Win rate = 2/4 = 0.5
    assert metrics["win_rate"] == 0.5
    # Total win = 300, total loss = 150 => profit_factor = 300/150 = 2
    assert metrics["profit_factor"] == 2
    # max_drawdown check: equity curve would be [100, 50, 250, 150]
    # rolling max is [100, 100, 250, 250], so drawdown is [0, -50, 0, -100]. min = -100
    assert metrics["max_drawdown"] == -100


def test_backtest_opening_range_breakout():
    """
    Provide a small sample DataFrame to test the breakout logic.
    We'll simulate a single day, during DST (e.g., June).
    """
    # We'll create a date in June to force DST
    date_str = "2023-06-15"
    times = pd.date_range(f"{date_str} 13:30", periods=10, freq="30T", tz="UTC")
    # times => 13:30, 14:00, 14:30, ..., up to 18:30 UTC

    # Construct columns to force a breakout above the opening range
    data = {
        "Open": [100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
        "High": [100, 101, 103, 103, 104, 106, 107, 107, 108, 109],
        "Low":  [99,  100, 101, 102, 103, 104, 105, 106, 107, 108],
        "Close":[100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
        "Volume":[1000]*10
    }
    df_test = pd.DataFrame(data, index=times)

    # The function uses local times in DST logic, but we've set up times in UTC
    # We'll run a short open_range_minutes so we can see a breakout quickly
    trades = backtest_opening_range_breakout(df_test, open_range_minutes=30)

    # We expect at least 1 trade: a breakout above the opening range around the 14:30 bar
    # Check the structure
    assert isinstance(trades, pd.DataFrame)
    if not trades.empty:
        assert "direction" in trades.columns
        assert "pnl" in trades.columns
        # The direction should be "long" because we break above the opening range
        assert trades.iloc[0]["direction"] == "long"
    else:
        pytest.fail("Expected at least one trade, but got none.")


@patch("analysis.opening_range_breakout.fetch_intraday_data")
def test_run_opening_range_breakout_tests(mock_fetch):
    """
    Test run_opening_range_breakout_tests by mocking fetch_intraday_data
    so we don't make real network calls. Provide a small DataFrame.
    """
    # Create a small DataFrame as mock data
    date_str = "2023-06-15"
    times = pd.date_range(f"{date_str} 13:30", periods=3, freq="5T", tz="UTC")
    data = {
        "Open": [100, 101, 102],
        "High": [101, 102, 103],
        "Low":  [99, 100, 101],
        "Close":[100, 101, 102],
        "Volume":[1000, 1200, 1100]
    }
    df_mock = pd.DataFrame(data, index=times)

    mock_fetch.return_value = df_mock

    # Call the function
    result = run_opening_range_breakout_tests("TSLA", days=1, interval="5m")

    # The result should have a "scenarios" list and "intraday_data" list
    assert "scenarios" in result
    assert "intraday_data" in result
    assert isinstance(result["scenarios"], list)
    assert isinstance(result["intraday_data"], list)

    # Each scenario in "scenarios" should have the expected fields
    for scenario in result["scenarios"]:
        for field in (
            "scenario_name", "filters", "open_range_minutes",
            "win_rate", "profit_factor", "sharpe_ratio", "max_drawdown",
            "num_trades", "net_pnl"
        ):
            assert field in scenario

    # intraday_data should match the columns we renamed
    # ("open", "high", "low", "close", "volume", etc.)
    if result["intraday_data"]:
        sample_row = result["intraday_data"][0]
        assert "open" in sample_row
        assert "high" in sample_row
        assert "low" in sample_row
        assert "close" in sample_row
        assert "volume" in sample_row
