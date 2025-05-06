# tests/test_backtest_strategies.py
import pandas as pd
import numpy as np
from unittest.mock import patch

# Project imports
from backtest_strategies.metrics import compute_metrics
from backtest_strategies.orb_strategies import backtest_orb
from backtest_strategies.runner import run_backtest_grid


# ───────────────────────────────────────────────────────────────
# helpers
# ───────────────────────────────────────────────────────────────
def _build_or_levels(df: pd.DataFrame, or_minutes: int) -> dict[int, dict[str, tuple[float, float]]]:
    """
    Minimal stand‑in for runner._build_or_lookup
        {or_minutes: {date_str: (or_high, or_low)}}
    """
    day_str = df.index[0].strftime("%Y-%m-%d")
    bar_min = int((df.index[1] - df.index[0]).seconds / 60)
    or_bars = max(1, or_minutes // bar_min)
    block   = df.iloc[:or_bars]
    return {or_minutes: {day_str: (block["high"].max(), block["low"].min())}}


# ───────────────────────────────────────────────────────────────
# metric function sanity‑check
# ───────────────────────────────────────────────────────────────
def test_compute_metrics():
    """Basic smoke‑test for compute_metrics."""
    trades = pd.DataFrame({"pnl": [100, -50, 200, -100]})
    metrics = compute_metrics(trades)

    required = {"win_rate", "profit_factor", "sharpe_ratio",
                "max_drawdown", "num_trades"}
    assert required.issubset(metrics.keys())

    assert metrics["num_trades"]   == 4
    assert metrics["win_rate"]     == 0.5          # 2 wins / 4 trades
    assert metrics["profit_factor"] == 2           # 300 / 150
    assert metrics["max_drawdown"] == -100         # equity: [100, 50, 250, 150]


# ───────────────────────────────────────────────────────────────
# ORB logic
# ───────────────────────────────────────────────────────────────
def test_backtest_opening_range_breakout():
    """
    Tiny one‑day dataset: price climbs straight up, so ORB should open **long**.
    """
    date_str = "2023-06-15"
    times = pd.date_range(f"{date_str} 13:30", periods=10, freq="30T", tz="UTC")

    df_test = pd.DataFrame({
        "open":   [100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
        "high":   [100, 101, 103, 103, 104, 106, 107, 107, 108, 109],
        "low":    [ 99, 100, 101, 102, 103, 104, 105, 106, 107, 108],
        "close":  [100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
        "volume": [1000] * 10,
    }, index=times)

    # ─── minimal extra columns expected by backtest_orb ───
    df_test["atr"]        = 1.0
    df_test["vwap"]       = df_test["close"]
    df_test["BB_upper"]   = np.nan
    df_test["BB_lower"]   = np.nan
    df_test["support"]    = np.nan
    df_test["resistance"] = np.nan

    # NEW: trading‑day key expected by updated orb logic
    df_test["trade_date"] = df_test.index.date

    or_minutes = 30
    or_levels  = _build_or_levels(df_test, or_minutes)

    trades = backtest_orb(
        df_test,
        or_levels=or_levels,
        open_range_minutes=or_minutes,
        use_volume_filter=False,
        use_vwap_filter=False,
        stop_loss=None,
        atr_stop_multiplier=None,
        time_exit_minutes=None,
        use_bb_exit=False,
        use_sr_exit=False,
        limit_same_direction=False,
        max_entries=1,
    )

    assert isinstance(trades, pd.DataFrame)
    assert not trades.empty, "Expected at least one trade."
    assert trades.iloc[0]["direction"] == "long"


# ───────────────────────────────────────────────────────────────
# end‑to‑end grid runner
# ───────────────────────────────────────────────────────────────
@patch("backtest_strategies.data_fetcher.fetch_intraday_data")
def test_run_backtest(mock_fetch):
    """
    Exercise run_backtest_grid end‑to‑end with a mocked data‑fetcher.
    """
    date_str = "2023-06-15"
    times = pd.date_range(f"{date_str} 13:30", periods=3, freq="5T", tz="UTC")
    df_mock = pd.DataFrame({
        # NOTE: lower‑case column names match the real pipeline
        "open":   [100, 101, 102],
        "high":   [101, 102, 103],
        "low":    [ 99, 100, 101],
        "close":  [100, 101, 102],
        "volume": [1000, 1200, 1100],
    }, index=times)
    mock_fetch.return_value = df_mock

    result = run_backtest_grid("TSLA", days=1, interval="5m")

    assert {"scenarios", "intraday_data"}.issubset(result.keys())
    assert isinstance(result["scenarios"], list)
    assert isinstance(result["intraday_data"], list)

    required_fields = {
        "strategy", "filters", "open_range_minutes",
        "win_rate", "profit_factor", "sharpe_ratio",
        "max_drawdown", "num_trades", "net_pnl",
    }
    for scenario in result["scenarios"]:
        assert required_fields.issubset(scenario.keys())

    # intraday rows now carry lower‑case OHLCV names
    if result["intraday_data"]:
        sample = result["intraday_data"][0]
        for k in ("open", "high", "low", "close", "volume"):
            assert k in sample
