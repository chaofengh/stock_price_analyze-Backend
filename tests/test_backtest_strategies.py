# tests/test_backtest_feature.py

from pathlib import Path
import math
from typing import List, Tuple, Any
import pandas as pd
import numpy as np
import pytest
from unittest.mock import patch

# ────────────────── project imports (keep grouped) ──────────────────
from backtest_strategies.metrics import compute_metrics
from backtest_strategies.bb_sr_strategies import (
    backtest_bbands,
    backtest_support_resistance,
)
from backtest_strategies.orb_strategies import (
    backtest_orb,
    backtest_reverse_orb,
)
from backtest_strategies.runner import (
    _build_or_lookup,
    run_backtest_grid,
)
from analysis.indicators import (
    compute_bollinger_bands,
    compute_realtime_sr,
)

# ───────────────────────── helper utilities ─────────────────────────


def _flatten_multiheader(df: pd.DataFrame) -> pd.DataFrame:
    """If CSV has ticker/field two‑row header, squash to first row."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]
    return df


def _add_min_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure every column referenced by any engine exists."""
    df = df.copy()
    df["atr"] = 1.0
    df[["vwap", "BB_upper", "BB_lower", "BB_middle"]] = np.nan
    df[["support", "resistance"]] = np.nan
    df["vwap"] = df["close"]
    df["trade_date"] = df.index.date
    return df


def _synthetic_frame(
    close: List[float],
    *,
    freq: str = "5min",
    tz: str = "UTC",
    sr: Tuple[float | None, float | None] = (None, None),
) -> pd.DataFrame:
    """
    Build a tiny OHLCV dataframe with all required placeholder feature columns.
    Pass `sr=(support, resistance)` to attach constant levels.
    """
    idx = pd.date_range("2025-05-07T13:30Z", periods=len(close), freq=freq, tz=tz)
    data = {
        "open": close,
        "high": np.array(close) + 0.05,
        "low": np.array(close) - 0.05,
        "close": close,
        "volume": 1_000,
    }
    df = pd.DataFrame(data, index=idx)
    df = _add_min_cols(df)

    sup, res = sr
    if sup is not None:
        df["support"] = sup
    if res is not None:
        df["resistance"] = res
    return df


def _set_constant_bbands(df: pd.DataFrame, *, lower: float, upper: float) -> pd.DataFrame:
    """Attach horizontal Bollinger bands to `df` so touch logic is deterministic."""
    df["BB_lower"] = lower
    df["BB_upper"] = upper
    df["BB_middle"] = (lower + upper) / 2
    return df


_EMPTY_KW: dict[str, Any] = dict(
    open_range_minutes=15,
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

# ──────────────────────────── fixtures ────────────────────────────


@pytest.fixture(scope="session")
def qqq_df_raw() -> pd.DataFrame:
    """Load 5‑minute QQQ candles in UTC from the CSV next to this file."""
    p = Path(__file__).with_name("QQQ_price_movement.csv")
    raw = pd.read_csv(p, header=[0, 1])
    raw = _flatten_multiheader(raw)
    raw.columns = [c.strip().lower() for c in raw.columns]
    raw.rename(columns={"datetime": "timestamp"}, inplace=True)
    raw["timestamp"] = pd.to_datetime(raw["timestamp"], utc=True)
    raw.set_index("timestamp", inplace=True)
    return raw


@pytest.fixture(scope="session")
def qqq_df_prepped(qqq_df_raw) -> pd.DataFrame:
    """
    Real QQQ frame with Bollinger & SR levels so stand‑alone engines run
    (keeps integration tests offline).
    """
    df = compute_bollinger_bands(qqq_df_raw.copy())
    df = compute_realtime_sr(df)
    df["trade_date"] = df.index.date
    df["atr"] = 1.0
    df["vwap"] = df["close"]
    return df.dropna(subset=["BB_lower", "BB_upper"])


# ──────────────────────── metric edge‑cases ───────────────────────


@pytest.mark.parametrize(
    "pnl, checker",
    [
        ([], lambda m: m["num_trades"] == 0 and m["win_rate"] == 0),
        ([0, 0, 0], lambda m: math.isinf(m["profit_factor"])),
        (
            [10, -5, 15],
            lambda m: m["win_rate"] == 2 / 3 and m["max_drawdown"] == -5,
        ),
    ],
)
def test_compute_metrics_edge_cases(pnl, checker):
    metrics = compute_metrics(pd.DataFrame({"pnl": pnl}))
    assert checker(metrics)


# ───────────── empty & single‑row guard‑rail tests ─────────────


@pytest.mark.parametrize(
    "engine",
    [backtest_orb, backtest_reverse_orb, backtest_bbands, backtest_support_resistance],
)
def test_engines_accept_empty(engine):
    if engine in (backtest_orb, backtest_reverse_orb):
        res = engine(pd.DataFrame(), or_levels={15: {}}, **_EMPTY_KW)
    else:
        res = engine(pd.DataFrame())
    assert res.empty


@pytest.mark.parametrize(
    "engine",
    [backtest_orb, backtest_reverse_orb, backtest_bbands, backtest_support_resistance],
)
def test_engines_accept_single_row(engine):
    df = _synthetic_frame([100])
    if engine in (backtest_orb, backtest_reverse_orb):
        res = engine(df, or_levels={15: {}}, **_EMPTY_KW)
    else:
        res = engine(df)
    assert res.empty


def test_missing_column_raises():
    """Dropping a referenced column should raise KeyError or AttributeError."""
    df = _synthetic_frame([100])
    df = df.drop(columns=["close"])
    with pytest.raises((KeyError, AttributeError)):
        backtest_bbands(df)


# ───────────── Bollinger mean‑reversion strategy ──────────────


def test_bbands_long_entry_and_midline_exit():
    prices = [100.5, 99.4, 99.9]  # bar‑1 dips below lower band once
    df = _synthetic_frame(prices)
    df = _set_constant_bbands(df, lower=100, upper=102)
    trades = backtest_bbands(df)
    assert len(trades) == 1
    tr = trades.iloc[0]
    assert tr.direction == "long"
    exit_ts = pd.to_datetime(tr.exit_time)
    assert exit_ts == df.index[-1]  # exited at last bar (midline never hit)


def test_bbands_short_entry_upper_band():
    prices = [99.0, 101.5, 100.2]  # bar‑1 above upper band once
    df = _synthetic_frame(prices)
    df = _set_constant_bbands(df, lower=97, upper=100)
    trades = backtest_bbands(df)
    assert len(trades) == 1 and trades.iloc[0].direction == "short"


def test_bbands_last_bar_exit_if_target_unhit():
    prices = [99.4, 99.2, 99.1]  # always below midline
    df = _synthetic_frame(prices)
    df = _set_constant_bbands(df, lower=100, upper=102)
    trades = backtest_bbands(df)
    exit_time = pd.to_datetime(trades.iloc[0].exit_time)
    assert exit_time == df.index[-1]


def test_bbands_no_reentry_while_open():
    prices = [100.5, 99.3, 100.1, 100.4, 100.6]  # single touch cycle
    df = _synthetic_frame(prices)
    df = _set_constant_bbands(df, lower=100, upper=102)
    trades = backtest_bbands(df)
    assert len(trades) == 1


# ───────────── Support / Resistance breakout strategy ─────────────


def test_sr_long_breakout():
    prices = [100, 101, 103, 104]  # breakout above 102.5
    df = _synthetic_frame(prices, sr=(None, 102.5))
    trades = backtest_support_resistance(df)
    assert len(trades) == 1 and trades.iloc[0].direction == "long"


def test_sr_short_breakout():
    prices = [103, 102, 100, 99]  # breakout below support
    df = _synthetic_frame(prices, sr=(101.5, None))
    trades = backtest_support_resistance(df)
    assert len(trades) == 1 and trades.iloc[0].direction == "short"


def test_sr_stop_reversal_long():
    prices = [104.5, 105, 102.8]  # fall below stored support
    df = _synthetic_frame(prices, sr=(103, 104))
    trades = backtest_support_resistance(df)
    tr = trades.iloc[0]
    assert tr.direction == "long" and tr.pnl < 0


def test_sr_stop_reversal_short():
    prices = [99.0, 98.5, 100.6]  # rise above stored resistance
    df = _synthetic_frame(prices, sr=(99.5, 101))
    trades = backtest_support_resistance(df)
    tr = trades.iloc[0]
    assert tr.direction == "short" and tr.pnl < 0


# ──────────────── _build_or_lookup helper tests ────────────────


def _daily_session(prices: List[float], day: str) -> pd.DataFrame:
    """Create a 1‑minute‑bar intraday session for helper tests."""
    idx = pd.date_range(f"{day}T13:30Z", periods=len(prices), freq="1min", tz="UTC")
    df = pd.DataFrame(
        {
            "open": prices,
            "high": np.array(prices) + 0.1,
            "low": np.array(prices) - 0.1,
            "close": prices,
            "volume": 10,
        },
        index=idx,
    )
    return _add_min_cols(df)


def test_build_or_lookup_returns_expected_keys():
    df = pd.concat(
        [
            _daily_session([1, 2, 3, 4], "2025-05-05"),
            _daily_session([2, 3, 4, 5], "2025-05-06"),
        ]
    )
    lookup = _build_or_lookup(df, [5, 15])
    assert set(lookup.keys()) == {5, 15}
    assert set(lookup[5].keys()) == {"2025-05-05", "2025-05-06"}


def test_build_or_lookup_ignores_short_days():
    df = _daily_session([1], "2025-05-07")  # only one bar
    lookup = _build_or_lookup(df, [15])
    assert "2025-05-07" not in lookup[15]


def test_build_or_lookup_respects_nan_in_or_block():
    df = _daily_session([np.nan, np.nan, np.nan], "2025-05-08")
    lookup = _build_or_lookup(df, [5])
    assert "2025-05-08" not in lookup[5]


# ─────────────── integration on real QQQ data ────────────────


def test_bbands_produces_trades_on_real_data(qqq_df_prepped):
    trades = backtest_bbands(qqq_df_prepped)
    assert len(trades) > 0 and np.isfinite(trades.pnl.sum())


def test_sr_produces_trades_on_real_data(qqq_df_prepped):
    trades = backtest_support_resistance(qqq_df_prepped)
    assert len(trades) > 0


def test_entry_exit_order(qqq_df_prepped):
    trades = backtest_bbands(qqq_df_prepped.head(500))
    assert (pd.to_datetime(trades.entry_time) <= pd.to_datetime(trades.exit_time)).all()


def test_run_backtest_grid_integration(qqq_df_raw):
    """Patch fetch_intraday_data so grid‑runner works offline."""
    from backtest_strategies import runner

    def _fake_fetch(*_, **__):
        return qqq_df_raw.copy()

    with patch.object(runner, "fetch_intraday_data", _fake_fetch):
        res = run_backtest_grid("QQQ", days="7d", interval="5m", top_n=5)

    candles = res["intraday_data"]
    assert len(candles) == len(qqq_df_raw)
    scenarios = res["scenarios"]
    assert len(scenarios) == 5
    # Ensure ordering by win‑rate then profit factor
    win_rates = [s["win_rate"] for s in scenarios]
    assert win_rates == sorted(win_rates, reverse=True)


# ──────────────── edge‑condition / misc tests ────────────────


def test_zero_volatility_creates_no_trades():
    prices = [100] * 20
    df_plain = _synthetic_frame(prices)
    df_sr = _synthetic_frame(prices, sr=(100, 100))
    assert backtest_bbands(df_plain).empty
    assert backtest_support_resistance(df_sr).empty


def test_max_entries_respected():
    df = _synthetic_frame([90, 91, 92, 93, 94, 95])
    # create one valid OR cross after open‑range bars
    df["atr"] = 0  # simplify stop logic
    df.loc[df.index[[1, 4]], "close"] = 80  # two dips but only one allowed
    or_levels = {15: {df.index[0].date().isoformat(): (96, 89)}}
    kw = _EMPTY_KW.copy()
    kw["max_entries"] = 1
    trades = backtest_orb(df, or_levels=or_levels, **kw)
    assert len(trades) == 1
