import numpy as np
import pandas as pd
import pytest

from analysis.indicators import compute_bollinger_bands
from analysis.trade_entry_evaluation import (
    _classify_setup,
    build_entry_decision_from_frame,
    run_decision_backtest,
)


def _base_frame(rows: int = 80) -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-02", periods=rows)
    x = np.linspace(0, 6 * np.pi, rows)
    close = 100 + np.sin(x) * 2 + np.linspace(0, 3.5, rows)
    open_ = close + np.cos(x) * 0.2
    high = np.maximum(open_, close) + 0.6
    low = np.minimum(open_, close) - 0.6
    volume = 1_000_000 + (np.sin(x) * 80_000)

    df = pd.DataFrame(
        {
            "date": dates,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        }
    )
    return compute_bollinger_bands(df)


def _force_lower_touch(frame: pd.DataFrame) -> pd.DataFrame:
    df = frame.copy()
    idx = df.index[-1]
    lower = float(df.loc[idx, "BB_lower"])
    df.loc[idx, "low"] = lower - 1.0
    df.loc[idx, "close"] = lower + 0.08
    df.loc[idx, "open"] = lower + 0.10
    df.loc[idx, "high"] = lower + 0.7
    return df


def _force_no_touch(frame: pd.DataFrame) -> pd.DataFrame:
    df = frame.copy()
    idx = df.index[-1]
    upper = float(df.loc[idx, "BB_upper"])
    lower = float(df.loc[idx, "BB_lower"])
    mid = float(df.loc[idx, "BB_middle"])
    df.loc[idx, "high"] = min(upper - 0.2, mid + 0.4)
    df.loc[idx, "low"] = max(lower + 0.2, mid - 0.4)
    df.loc[idx, "open"] = mid
    df.loc[idx, "close"] = mid
    return df


def test_stage_a_earnings_block_disables_favorable_regime():
    df = _force_lower_touch(_base_frame())
    earnings_dates = {pd.Timestamp(df["date"].iloc[-1]).normalize()}

    payload = build_entry_decision_from_frame("TEST", df, earnings_dates=earnings_dates)

    assert payload["stage_a"]["event_risk_blocked"] is True
    assert payload["stage_a"]["is_favorable"] is False


def test_probabilities_are_bounded_between_zero_and_one():
    df = _force_lower_touch(_base_frame())
    payload = build_entry_decision_from_frame("TEST", df, earnings_dates=set())

    assert 0.0 <= payload["stage_a"]["probability"] <= 1.0
    assert 0.0 <= payload["stage_b"]["entry_probability"] <= 1.0
    assert 0.0 <= payload["reversion_probability"] <= 1.0
    assert 0.0 <= payload["continuation_probability"] <= 1.0


@pytest.mark.parametrize(
    "touched_side,enter_today,expected",
    [
        (None, False, "no_band_setup"),
        ("Upper", True, "upper_band_mean_reversion"),
        ("Lower", True, "lower_band_mean_reversion"),
        ("Upper", False, "trend_continuation_trap"),
        ("Lower", False, "trend_continuation_trap"),
    ],
)
def test_setup_classification_mapping(touched_side, enter_today, expected):
    assert _classify_setup(touched_side, enter_today) == expected


def test_no_touch_maps_to_no_band_setup_in_payload():
    df = _force_no_touch(_base_frame())
    payload = build_entry_decision_from_frame("TEST", df, earnings_dates=set())

    assert payload["touched_side"] is None
    assert payload["setup_type"] == "no_band_setup"


def test_top_reasons_are_ranked_by_absolute_contribution_and_capped_at_five():
    df = _force_lower_touch(_base_frame())
    payload = build_entry_decision_from_frame("TEST", df, earnings_dates=set())

    reasons = payload["top_reasons"]
    assert len(reasons) <= 5

    magnitudes = [abs(r["contribution"]) for r in reasons]
    assert magnitudes == sorted(magnitudes, reverse=True)


def test_accuracy_backtest_scoring_for_upper_lower_and_flat():
    df = pd.DataFrame(
        {
            "date": pd.bdate_range("2025-01-02", periods=5),
            "close": [100.0, 99.0, 100.0, 100.0, 99.0],
            "touched_side": ["Upper", "Lower", "Upper", "Lower", None],
        }
    )

    # Predicted class must map from final decision (enter_today), not raw probabilities.
    decisions = {
        0: {"enter_today": True, "stage_a": {"probability": 0.64}, "reversion_probability": 0.7},
        1: {"enter_today": False, "stage_a": {"probability": 0.62}, "reversion_probability": 0.9},
        2: {"enter_today": True, "stage_a": {"probability": 0.60}, "reversion_probability": 0.8},
        3: {"enter_today": False, "stage_a": {"probability": 0.58}, "reversion_probability": 0.2},
    }

    backtest = run_decision_backtest(df, decisions_by_index=decisions)

    assert backtest["sample_count"] == 4
    assert backtest["correct_count"] == 2
    assert backtest["accuracy"] == 0.5
    assert backtest["reverse_call_count"] == 2
    assert backtest["continue_call_count"] == 2
    assert backtest["reverse_precision"] == 0.5
    assert backtest["continue_precision"] == 0.5
    assert backtest["tp_reverse"] == 1
    assert backtest["fp_reverse"] == 1
    assert backtest["tn_reverse"] == 1
    assert backtest["fn_reverse"] == 1
    assert backtest["flat_count"] == 1

    by_date = {item["signal_date"]: item for item in backtest["recent_predictions"]}
    assert by_date["2025-01-03"]["predicted_class"] == "continue"
    assert by_date["2025-01-06"]["actual_next_day_direction"] == "flat"
    assert by_date["2025-01-06"]["is_correct"] is False


def test_as_of_date_exact_trading_day_uses_same_date():
    df = _force_lower_touch(_base_frame())
    target_date = pd.Timestamp(df["date"].iloc[-5]).strftime("%Y-%m-%d")

    payload = build_entry_decision_from_frame("TEST", df, as_of_date=target_date, earnings_dates=set())

    assert payload["requested_as_of_date"] == target_date
    assert payload["as_of_date"] == target_date
    assert payload["date_was_snapped"] is False


def test_as_of_date_weekend_snaps_to_previous_trading_day():
    df = _force_lower_touch(_base_frame())
    trading_dates = pd.DatetimeIndex(pd.to_datetime(df["date"])).normalize()
    friday = [d for d in trading_dates[-20:] if d.weekday() == 4][-1]
    saturday = (friday + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    friday_str = friday.strftime("%Y-%m-%d")

    payload = build_entry_decision_from_frame("TEST", df, as_of_date=saturday, earnings_dates=set())

    assert payload["requested_as_of_date"] == saturday
    assert payload["as_of_date"] == friday_str
    assert payload["date_was_snapped"] is True


def test_as_of_date_out_of_range_raises_validation_error():
    df = _force_lower_touch(_base_frame())

    with pytest.raises(ValueError, match="outside available range"):
        build_entry_decision_from_frame("TEST", df, as_of_date="2024-01-01", earnings_dates=set())


def test_as_of_date_invalid_format_raises_validation_error():
    df = _force_lower_touch(_base_frame())

    with pytest.raises(ValueError, match="Expected YYYY-MM-DD"):
        build_entry_decision_from_frame("TEST", df, as_of_date="04/15/2026", earnings_dates=set())
