from datetime import datetime
import logging
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import pytest
import pytz

import analysis.trade_entry_evaluation as tee
import analysis.trade_entry_evaluation.payload as tee_payload
from analysis.indicators import compute_bollinger_bands
from analysis.trade_entry_evaluation import (
    _MODEL_FEATURES,
    _apply_deployment_quality_gates_to_decision,
    _apply_direction_quality_gates_to_decision,
    _adaptive_state,
    _adaptive_training_indices,
    _build_training_matrix,
    _coverage_repair_candidates,
    _coverage_repair_horizon,
    _deployment_quality_gate,
    _ensure_decision_for_index,
    _finalize_deployment_quality,
    _prepare_feature_frame,
    _prediction_book_metrics,
    _prediction_book_preserves_accuracy,
    _prediction_evidence_candidates,
    _prior_completed_predictions,
    _quiet_yfinance_earnings_lookup,
    _reversal_veto_reason,
    _select_max_safe_coverage_candidates,
    _empirical_regime_attrs,
    _empirical_rows_for_scope,
    build_entry_decision_from_context,
    build_entry_decision_context_from_frame,
    build_entry_decision_from_frame,
    entry_decision_feature_schema_version,
    entry_decision_model_version,
    evaluate_entry_context_freshness,
    evaluate_row_decision,
    run_decision_backtest,
)


def _base_frame(rows: int = 100) -> pd.DataFrame:
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


def _force_no_touch(frame: pd.DataFrame, idx: int | None = None) -> pd.DataFrame:
    df = frame.copy()
    idx = df.index[-1] if idx is None else idx
    upper = float(df.loc[idx, "BB_upper"])
    lower = float(df.loc[idx, "BB_lower"])
    mid = float(df.loc[idx, "BB_middle"])
    df.loc[idx, "high"] = min(upper - 0.2, mid + 0.4)
    df.loc[idx, "low"] = max(lower + 0.2, mid - 0.4)
    df.loc[idx, "open"] = mid
    df.loc[idx, "close"] = mid
    return df


def _manual_horizon(status, predicted_direction=None, confidence=0.7, tier=None, signal_id=None):
    if status != "prediction":
        return {
            "status": "no_prediction",
            "predicted_direction": None,
            "continuation_probability": 0.52,
            "reversal_probability": 0.48,
            "confidence_score": 52,
            "no_prediction_reason": "low_confidence",
        }
    continuation_probability = confidence if predicted_direction == "continuation" else 1 - confidence
    horizon = {
        "status": "prediction",
        "predicted_direction": predicted_direction,
        "continuation_probability": continuation_probability,
        "reversal_probability": 1 - continuation_probability,
        "confidence_score": int(confidence * 100),
        "no_prediction_reason": None,
    }
    if tier:
        horizon["playbook"] = {"tier": tier, "id": signal_id or tier}
    elif signal_id:
        horizon["playbook"] = {"id": signal_id}
    return horizon


def _blocked_manual_horizon(predicted_direction: str, reason: str = "direction_quality_gate_failed") -> dict:
    horizon = _manual_horizon("no_prediction")
    horizon["no_prediction_reason"] = reason
    horizon["blocked_prediction"] = {
        "status": "prediction",
        "predicted_direction": predicted_direction,
        "confidence_score": 91,
        "continuation_probability": 0.91 if predicted_direction == "continuation" else 0.09,
        "reversal_probability": 0.91 if predicted_direction == "reversal" else 0.09,
        "no_prediction_reason": None,
    }
    return horizon


def _manual_decision(h5, h10):
    return {
        "touched_side": "Upper",
        "setup_type": "upper_band_touch",
        "event_risk_blocked": False,
        "event_risk": {"blocked": False},
        "horizons": {
            "5d": h5,
            "10d": h10,
        },
        "top_reasons": [],
    }


def _minimal_feature_context_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.bdate_range("2026-05-05", periods=3),
            "open": [100.0, 101.0, 102.0],
            "high": [102.0, 104.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [101.0, 103.0, 102.5],
            "BB_upper": [110.0, 102.0, 110.0],
            "BB_lower": [90.0, 92.0, 91.0],
            "BB_middle": [100.0, 97.0, 100.5],
            "ATR14": [2.0, 2.0, 2.0],
            "touched_side": [None, "Upper", None],
            "event_risk_blocked": [False, False, False],
            "event_risk_event_date": [None, None, None],
            "event_risk_sessions_to_event": [pd.NA, pd.NA, pd.NA],
            "event_risk_calendar_days_to_event": [pd.NA, pd.NA, pd.NA],
            "event_risk_reason": [None, None, None],
        }
    )


def _minimal_backtest_payload() -> dict:
    return {
        "5d": {"predictions": [], "recent_predictions": []},
        "10d": {"predictions": [], "recent_predictions": []},
    }


def _coverage_gate_frame(actual_by_index: dict[int, str]) -> pd.DataFrame:
    rows = max(actual_by_index) + 16
    df = pd.DataFrame(
        {
            "date": pd.bdate_range("2025-01-02", periods=rows),
            "close": [100.0] * rows,
            "touched_side": [None] * rows,
        }
    )
    for idx, actual in actual_by_index.items():
        df.loc[idx, "touched_side"] = "Upper"
        df.loc[idx, "close"] = 100.0
        df.loc[idx + 5, "close"] = 110.0 if actual == "continuation" else 90.0
    return df


def test_latest_no_touch_returns_no_prediction_for_both_horizons():
    df = _force_no_touch(_base_frame())
    payload = build_entry_decision_from_frame("TEST", df, earnings_dates=set())

    assert payload["touched_side"] is None
    assert payload["setup_type"] == "no_band_setup"
    assert payload["horizons"]["5d"]["status"] == "no_prediction"
    assert payload["horizons"]["5d"]["no_prediction_reason"] == "no_bollinger_touch"
    assert payload["horizons"]["10d"]["status"] == "no_prediction"
    assert payload["horizons"]["10d"]["no_prediction_reason"] == "no_bollinger_touch"


def test_historical_no_touch_returns_no_prediction_for_both_horizons():
    target_idx = 80
    df = _force_no_touch(_base_frame(110), idx=target_idx)
    target_date = pd.Timestamp(df["date"].iloc[target_idx]).strftime("%Y-%m-%d")
    payload = build_entry_decision_from_frame("TEST", df, as_of_date=target_date, earnings_dates=set())

    assert payload["touched_side"] is None
    assert payload["setup_type"] == "no_band_setup"
    assert payload["horizons"]["5d"]["status"] == "no_prediction"
    assert payload["horizons"]["5d"]["no_prediction_reason"] == "no_bollinger_touch"
    assert payload["horizons"]["10d"]["status"] == "no_prediction"
    assert payload["horizons"]["10d"]["no_prediction_reason"] == "no_bollinger_touch"


def _empty_supervised_frame(rows: int) -> pd.DataFrame:
    data = {
        "date": pd.bdate_range("2025-01-02", periods=rows),
        "close": np.full(rows, 100.0),
        "touched_side": [None] * rows,
        "event_risk_blocked": [False] * rows,
    }
    for feature in _MODEL_FEATURES:
        data[feature] = np.zeros(rows)
    return pd.DataFrame(data)


def _set_training_signal(df: pd.DataFrame, idx: int, *, continuation: bool, feature_value: float) -> None:
    df.loc[idx, "touched_side"] = "Upper"
    df.loc[idx, "touch_side_sign"] = 1.0
    df.loc[idx, "side_close_location"] = feature_value
    df.loc[idx, "touch_wick_minus_body"] = feature_value
    df.loc[idx, "side_directional_streak"] = feature_value
    df.loc[idx + 5, "close"] = 112.0 if continuation else 88.0


def _build_learnable_supervised_frame(*, target_feature_value: float = 2.5) -> tuple[pd.DataFrame, int]:
    target_idx = 150
    df = _empty_supervised_frame(170)
    signal_indices = list(range(0, 144, 6))
    for offset, idx in enumerate(signal_indices):
        continuation = offset % 2 == 0
        feature_value = 2.5 if continuation else -2.5
        _set_training_signal(df, idx, continuation=continuation, feature_value=feature_value)

    df.loc[target_idx, "touched_side"] = "Upper"
    df.loc[target_idx, "touch_side_sign"] = 1.0
    df.loc[target_idx, "side_close_location"] = target_feature_value
    df.loc[target_idx, "touch_wick_minus_body"] = target_feature_value
    df.loc[target_idx, "side_directional_streak"] = target_feature_value
    return df, target_idx


def _build_continuation_analog_frame() -> tuple[pd.DataFrame, int]:
    rows = 130
    target_idx = 112
    x = np.arange(rows, dtype=float)
    close = 100.0 + (np.sin(x / 3.0) * 4.0) + (np.cos(x / 7.0) * 2.0)
    df = _empty_supervised_frame(rows)
    df["close"] = close
    df["touched_side"] = "Upper"
    df["touch_side_sign"] = 1.0
    df["analysis_side_sign"] = 1.0
    df["event_risk_blocked"] = False
    df["touch_reentry_signal"] = -1.0
    df["touch_wick_minus_body"] = 0.0
    df["side_close_location"] = 0.65
    df["ADX14"] = 24.0

    for idx in range(rows - 10):
        label = 1 if close[idx + 5] > close[idx] else 0
        feature_value = 2.5 if label == 1 else -2.5
        df.loc[idx, "side_ret_5d"] = feature_value
        df.loc[idx, "side_ret_10d"] = feature_value
        df.loc[idx, "side_ma20_slope_5"] = feature_value
        df.loc[idx, "side_directional_streak"] = feature_value
        df.loc[idx, "side_qqq_ret_5d"] = feature_value
        df.loc[idx, "side_xlk_ret_5d"] = feature_value

    for feature in (
        "side_ret_5d",
        "side_ret_10d",
        "side_ma20_slope_5",
        "side_directional_streak",
        "side_qqq_ret_5d",
        "side_xlk_ret_5d",
    ):
        df.loc[target_idx, feature] = 2.5

    return df, target_idx


def test_touch_without_training_data_does_not_force_prediction():
    row = pd.Series(
        {
            "touched_side": "Upper",
            "touch_reentry_signal": 0.0,
            "close_in_range": 0.5,
            "signed_close_location": 0.0,
            "upper_wick_ratio": 0.3,
            "body_pct": 0.3,
            "ADX14": 22.0,
            "trend_alignment": 0.0,
            "ma20_slope_5": 0.0,
            "ma50_slope_5": 0.0,
            "MACD_hist_atr": 0.0,
            "directional_streak": 0.0,
            "consecutive_touch_count": 1.0,
            "volume_range_interaction": 1.10,
            "rel_volume_20": 1.0,
            "volume_zscore_20": 0.0,
            "weighted_volume_pressure_5": 0.0,
            "obv_slope_5": 0.0,
            "bandwidth_change_3d": 0.0,
            "band_width_percentile": 0.5,
            "realized_vol_percentile": 0.5,
            "RSI14": 50.0,
            "MFI14": 50.0,
            "CCI20": 0.0,
            "gap_atr": 0.0,
            "range_expansion_5": 1.0,
        }
    )

    decision = evaluate_row_decision(row)

    assert decision["horizons"]["5d"]["status"] == "no_prediction"
    assert decision["horizons"]["5d"]["no_prediction_reason"] == "insufficient_training_data"
    assert decision["horizons"]["10d"]["status"] == "no_prediction"
    assert decision["horizons"]["10d"]["no_prediction_reason"] == "insufficient_training_data"


def test_adaptive_model_requires_enough_training_history():
    df, target_idx = _build_learnable_supervised_frame(target_feature_value=2.5)

    decision = evaluate_row_decision(df.iloc[target_idx], feature_df=df, row_index=target_idx)

    horizon = decision["horizons"]["5d"]
    assert horizon["model"]["training_sample_count"] == 24
    assert horizon["status"] == "no_prediction"
    assert horizon["no_prediction_reason"] == "insufficient_training_data"
    assert horizon["model"]["type"] == "walk_forward_adaptive_analog"
    assert horizon["model"]["continuation_training_count"] == 12
    assert horizon["model"]["reversal_training_count"] == 12


def test_adaptive_model_reports_low_history_instead_of_forcing_prediction():
    df, target_idx = _build_learnable_supervised_frame(target_feature_value=0.0)

    decision = evaluate_row_decision(df.iloc[target_idx], feature_df=df, row_index=target_idx)

    horizon = decision["horizons"]["5d"]
    assert horizon["model"]["training_sample_count"] == 24
    assert horizon["status"] == "no_prediction"
    assert horizon["no_prediction_reason"] == "insufficient_training_data"


def test_training_matrix_uses_only_touch_rows_when_outcomes_are_known():
    df = _empty_supervised_frame(30)
    df["close"] = np.linspace(100.0, 129.0, 30)
    df["analysis_side_sign"] = 1.0
    df.loc[[0, 2, 4, 6], "touched_side"] = "Upper"
    df.loc[[0, 2, 4, 6], "touch_side_sign"] = 1.0

    x_train, y_train, indices = _build_training_matrix(df, target_idx=20, horizon=5)

    assert len(x_train) == 4
    assert len(y_train) == 4
    assert indices.tolist() == [0, 2, 4, 6]
    assert df.loc[indices, "touched_side"].eq("Upper").all()
    assert set(y_train.tolist()) == {1.0}


def test_training_matrix_excludes_rows_whose_outcome_is_after_target_date():
    df = _empty_supervised_frame(25)
    for idx, continuation in ((0, True), (5, False), (6, True), (10, True)):
        _set_training_signal(df, idx, continuation=continuation, feature_value=float(idx))

    _x_train, y_train, indices = _build_training_matrix(df, target_idx=10, horizon=5)

    assert indices.tolist() == [0, 5]
    assert y_train.tolist() == [1.0, 0.0]


def test_adaptive_state_labels_do_not_leak_into_training_indices():
    df = _empty_supervised_frame(25)
    for idx, continuation in ((0, True), (5, False), (6, True), (10, True)):
        _set_training_signal(df, idx, continuation=continuation, feature_value=float(idx))

    state = _adaptive_state(df, horizon=5)
    train_idx = _adaptive_training_indices(state, target_idx=10, horizon=5)

    assert np.isfinite(state["labels"][6])
    assert train_idx.tolist() == [0, 5]
    assert all(idx + 5 <= 10 for idx in train_idx)


def test_empirical_regime_rows_exclude_current_and_future_outcomes():
    df = _empty_supervised_frame(30)
    for idx, continuation in ((0, True), (5, False), (6, True), (12, True)):
        _set_training_signal(df, idx, continuation=continuation, feature_value=0.0)
    df.loc[10, "touched_side"] = "Upper"
    df.loc[10, "touch_side_sign"] = 1.0

    target_attrs = _empirical_regime_attrs(df.iloc[10], horizon=5)
    rows = _empirical_rows_for_scope(
        df,
        target_attrs,
        row_index=10,
        horizon=5,
        fields=("side",),
    )

    assert [row["idx"] for row in rows] == [0, 5]
    target_date = pd.Timestamp(df.loc[10, "date"])
    assert all(pd.Timestamp(row["outcome_date"]) <= target_date for row in rows)


def test_deployment_evidence_uses_only_prior_completed_outcomes():
    prediction = {
        "signal_date": "2026-01-10",
        "outcome_date": "2026-01-17",
        "horizon_days": 5,
        "touched_side": "Upper",
        "predicted_direction": "continuation",
        "actual_direction": "continuation",
        "is_correct": True,
        "confidence_score": 90,
        "signal_model_id": "same_signal",
        "signal_tier": "regime",
    }
    candidate = _prediction_evidence_candidates(prediction)[0]
    prior_rows = [
        {**prediction, "signal_date": "2025-12-30", "outcome_date": "2026-01-09"},
        {**prediction, "signal_date": "2026-01-03", "outcome_date": "2026-01-10"},
        {**prediction, "signal_date": "2026-01-04", "outcome_date": "2026-01-12"},
        {
            **prediction,
            "signal_date": "2025-12-30",
            "outcome_date": "2026-01-09",
            "signal_model_id": "different_signal",
        },
    ]

    rows = _prior_completed_predictions(
        prior_rows,
        scope=candidate["scope"],
        key=candidate["key"],
        signal_date=pd.Timestamp(prediction["signal_date"]),
    )

    assert len(rows) == 1
    assert rows[0]["outcome_date"] == "2026-01-09"
    assert rows[0]["signal_model_id"] == "same_signal"


def test_similar_case_payload_uses_training_side_for_non_touch_analog_rows():
    df = _empty_supervised_frame(12)
    df["close"] = 100.0
    df.loc[0, "analysis_side_sign"] = 1.0
    df.loc[5, "close"] = 110.0

    case = tee._similar_case_payload(df, 0, 5, "continuation", similarity=0.9)

    assert case["touched_side"] == "Upper"
    assert case["was_band_touch"] is False
    assert case["actual_direction"] == "continuation"
    assert case["is_correct"] is True
    assert case["trade_direction"] == "long"
    assert case["trade_return"] == pytest.approx(0.1)
    assert case["similarity"] == pytest.approx(0.9)


def test_adaptive_model_does_not_use_future_labeled_results():
    df, target_idx = _build_learnable_supervised_frame(target_feature_value=2.5)
    baseline = evaluate_row_decision(df.iloc[target_idx], feature_df=df, row_index=target_idx)

    modified = df.copy()
    future_signal_indices = list(range(target_idx + 6, target_idx + 6 + 48, 6))
    for idx in future_signal_indices:
        if idx + 5 >= len(modified):
            break
        _set_training_signal(modified, idx, continuation=False, feature_value=9.0)

    changed = evaluate_row_decision(modified.iloc[target_idx], feature_df=modified, row_index=target_idx)

    assert changed["horizons"]["5d"]["continuation_probability"] == pytest.approx(
        baseline["horizons"]["5d"]["continuation_probability"]
    )
    assert changed["horizons"]["5d"]["predicted_direction"] == baseline["horizons"]["5d"]["predicted_direction"]


def test_adaptive_model_can_make_validated_continuation_predictions():
    df, target_idx = _build_continuation_analog_frame()

    decision = evaluate_row_decision(df.iloc[target_idx], feature_df=df, row_index=target_idx)

    horizon = decision["horizons"]["5d"]
    assert horizon["status"] == "prediction"
    assert horizon["predicted_direction"] == "continuation"
    assert horizon["continuation_validation_precision"] >= 0.75
    assert horizon["playbook"]["tier"] in {"core", "expansion", "opportunity", "regime"}
    assert horizon["key_reasons"]
    assert horizon["key_reasons"][0]["feature"]
    assert horizon["similar_past_cases"]
    similar_case = horizon["similar_past_cases"][0]
    assert similar_case["signal_date"]
    assert similar_case["outcome_date"]
    assert similar_case["predicted_direction"] == "continuation"
    assert similar_case["actual_direction"] in {"continuation", "reversal"}
    assert similar_case["is_correct"] in {True, False}
    assert "trade_return" in similar_case


def test_continuation_signal_is_vetoed_when_band_rejection_is_strong():
    df, target_idx = _build_continuation_analog_frame()
    df.loc[target_idx, "touch_reentry_signal"] = 1.0
    df.loc[target_idx, "touch_wick_minus_body"] = 0.40
    df.loc[target_idx, "side_close_location"] = 0.10

    decision = evaluate_row_decision(df.iloc[target_idx], feature_df=df, row_index=target_idx)

    horizon = decision["horizons"]["5d"]
    assert horizon["predicted_direction"] != "continuation"
    if horizon["status"] == "no_prediction":
        assert horizon["no_prediction_reason"] == "continuation_rejected_at_band"


def test_probabilities_are_bounded_between_zero_and_one():
    df = _force_lower_touch(_base_frame())
    payload = build_entry_decision_from_frame("TEST", df, earnings_dates=set())

    for horizon in ("5d", "10d"):
        decision = payload["horizons"][horizon]
        assert 0.0 <= decision["continuation_probability"] <= 1.0
        assert 0.0 <= decision["reversal_probability"] <= 1.0


def test_top_reasons_are_ranked_by_absolute_contribution_and_capped_at_eight():
    df = _force_lower_touch(_base_frame())
    payload = build_entry_decision_from_frame("TEST", df, earnings_dates=set())

    reasons = payload["top_reasons"]
    assert len(reasons) <= 8

    magnitudes = [abs(r["contribution"]) for r in reasons]
    assert magnitudes == sorted(magnitudes, reverse=True)


def test_backtest_scores_5d_and_10d_direction_and_excludes_no_predictions():
    df = pd.DataFrame(
        {
            "date": pd.bdate_range("2025-01-02", periods=16),
            "close": [
                100.0,
                100.0,
                100.0,
                100.0,
                100.0,
                105.0,
                105.0,
                90.0,
                100.0,
                100.0,
                90.0,
                90.0,
                100.0,
                80.0,
                100.0,
                100.0,
            ],
            "touched_side": ["Upper", "Lower", "Upper", "Lower"] + [None] * 12,
        }
    )
    decisions = {
        0: _manual_decision(
            _manual_horizon("prediction", "continuation", tier="core"),
            _manual_horizon("prediction", "reversal", tier="expansion"),
        ),
        1: _manual_decision(
            _manual_horizon("prediction", "continuation", tier="expansion"),
            _manual_horizon("prediction", "continuation", tier="core"),
        ),
        2: _manual_decision(
            _manual_horizon("no_prediction"),
            _manual_horizon("no_prediction"),
        ),
        3: _manual_decision(
            _manual_horizon("prediction", "continuation"),
            _manual_horizon("prediction", "continuation"),
        ),
    }

    backtest = run_decision_backtest(df, decisions_by_index=decisions)

    assert backtest["5d"]["eligible_touch_count"] == 4
    assert backtest["5d"]["prediction_count"] == 3
    assert backtest["5d"]["sample_count"] == 3
    assert backtest["5d"]["no_prediction_count"] == 1
    assert backtest["5d"]["coverage"] == 0.75
    assert backtest["5d"]["correct_count"] == 1
    assert backtest["5d"]["accuracy"] == pytest.approx(1 / 3)
    assert backtest["5d"]["continuation_call_count"] == 3
    assert backtest["5d"]["continuation_correct_count"] == 1
    assert backtest["5d"]["continuation_accuracy"] == pytest.approx(1 / 3)
    assert backtest["5d"]["reversal_call_count"] == 0
    assert backtest["5d"]["reversal_accuracy"] is None
    assert backtest["5d"]["missed_reversal_count"] == 2
    assert backtest["5d"]["flat_count"] == 1
    assert backtest["5d"]["signal_tier_counts"] == {"core": 1, "expansion": 1}

    assert backtest["10d"]["eligible_touch_count"] == 4
    assert backtest["10d"]["prediction_count"] == 3
    assert backtest["10d"]["no_prediction_count"] == 1
    assert backtest["10d"]["correct_count"] == 3
    assert backtest["10d"]["accuracy"] == 1.0
    assert backtest["10d"]["continuation_call_count"] == 2
    assert backtest["10d"]["continuation_accuracy"] == 1.0
    assert backtest["10d"]["reversal_call_count"] == 1
    assert backtest["10d"]["reversal_correct_count"] == 1
    assert backtest["10d"]["reversal_accuracy"] == 1.0
    assert backtest["10d"]["missed_reversal_count"] == 0
    assert backtest["10d"]["signal_tier_counts"] == {"expansion": 1, "core": 1}

    by_date = {item["signal_date"]: item for item in backtest["10d"]["recent_predictions"]}
    assert by_date["2025-01-02"]["predicted_direction"] == "reversal"
    assert by_date["2025-01-03"]["actual_direction"] == "continuation"


def test_backtest_adds_expected_value_and_confidence_bucket_scoring():
    df = pd.DataFrame(
        {
            "date": pd.bdate_range("2025-01-02", periods=16),
            "close": [
                100.0,
                100.0,
                100.0,
                100.0,
                100.0,
                110.0,
                95.0,
                90.0,
                102.0,
                104.0,
                100.0,
                100.0,
                100.0,
                100.0,
                100.0,
                100.0,
            ],
            "ATR14": [10.0] * 16,
            "touched_side": ["Upper", "Upper", "Lower", "Lower", "Upper"] + [None] * 11,
        }
    )
    decisions = {
        0: _manual_decision(
            _manual_horizon("prediction", "continuation", confidence=0.64),
            _manual_horizon("no_prediction"),
        ),
        1: _manual_decision(
            _manual_horizon("prediction", "reversal", confidence=0.67),
            _manual_horizon("no_prediction"),
        ),
        2: _manual_decision(
            _manual_horizon("prediction", "continuation", confidence=0.84),
            _manual_horizon("no_prediction"),
        ),
        3: _manual_decision(
            _manual_horizon("prediction", "reversal", confidence=0.87),
            _manual_horizon("no_prediction"),
        ),
        4: _manual_decision(
            _manual_horizon("prediction", "reversal", confidence=0.94),
            _manual_horizon("no_prediction"),
        ),
    }

    backtest = run_decision_backtest(df, decisions_by_index=decisions)
    five_day = backtest["5d"]

    assert five_day["accuracy"] == 0.6
    assert five_day["win_rate"] == pytest.approx(0.8)
    assert five_day["expected_return"] == pytest.approx(0.046)
    assert five_day["expected_downside"] == pytest.approx(-0.04)
    assert five_day["expected_atr_return"] == pytest.approx(0.46)
    assert five_day["atr_reward_risk"] == pytest.approx(1.6875)
    assert five_day["flat_count"] == 2

    by_date = {item["signal_date"]: item for item in five_day["predictions"]}
    assert by_date["2025-01-08"]["is_correct"] is False
    assert by_date["2025-01-08"]["actual_direction"] == "flat"
    assert by_date["2025-01-08"]["trade_direction"] == "short"
    assert by_date["2025-01-08"]["trade_return"] == pytest.approx(-0.04)

    buckets = {item["bucket"]: item for item in five_day["confidence_buckets"]}
    assert buckets["60-69"]["prediction_count"] == 2
    assert buckets["60-69"]["accuracy"] == 1.0
    assert buckets["60-69"]["win_rate"] == 1.0
    assert buckets["60-69"]["expected_return"] == pytest.approx(0.075)
    assert buckets["80-89"]["prediction_count"] == 2
    assert buckets["80-89"]["accuracy"] == 0.5
    assert buckets["80-89"]["expected_return"] == pytest.approx(0.06)
    assert buckets["90-99"]["prediction_count"] == 1
    assert buckets["90-99"]["win_rate"] == 0.0
    assert buckets["90-99"]["expected_downside"] == pytest.approx(-0.04)


def test_backtest_excludes_rows_without_enough_future_data():
    df = pd.DataFrame(
        {
            "date": pd.bdate_range("2025-01-02", periods=12),
            "close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0, 110.0, 111.0],
            "touched_side": ["Upper"] + [None] * 9 + ["Upper", None],
        }
    )
    decisions = {
        0: _manual_decision(
            _manual_horizon("prediction", "continuation"),
            _manual_horizon("prediction", "continuation"),
        ),
        10: _manual_decision(
            _manual_horizon("prediction", "continuation"),
            _manual_horizon("prediction", "continuation"),
        ),
    }

    backtest = run_decision_backtest(df, decisions_by_index=decisions)

    assert backtest["5d"]["eligible_touch_count"] == 1
    assert backtest["5d"]["prediction_count"] == 1
    assert backtest["5d"]["incomplete_future_count"] == 1
    assert backtest["10d"]["eligible_touch_count"] == 1
    assert backtest["10d"]["prediction_count"] == 1
    assert backtest["10d"]["incomplete_future_count"] == 1


def test_upper_band_reversal_is_wrong_when_exit_close_is_flat_inside_atr_hurdle():
    df = pd.DataFrame(
        {
            "date": pd.bdate_range("2026-04-16", periods=12),
            "close": [100.0, 100.0, 100.0, 100.0, 100.0, 104.0, 100.0, 100.0, 100.0, 100.0, 112.0, 100.0],
            "ATR14": [10.0] * 12,
            "touched_side": ["Upper"] + [None] * 11,
        }
    )
    decisions = {
        0: _manual_decision(
            _manual_horizon("prediction", "reversal"),
            _manual_horizon("prediction", "reversal"),
        )
    }

    backtest = run_decision_backtest(df, decisions_by_index=decisions)

    assert backtest["5d"]["prediction_count"] == 1
    assert backtest["5d"]["correct_count"] == 0
    assert backtest["5d"]["reversal_accuracy"] == 0.0
    assert backtest["5d"]["flat_count"] == 1
    prediction = backtest["5d"]["predictions"][0]
    assert prediction["signal_date"] == "2026-04-16"
    assert prediction["outcome_date"] == "2026-04-23"
    assert prediction["actual_direction"] == "flat"
    assert prediction["is_correct"] is False
    assert prediction["trade_direction"] == "short"
    assert prediction["trade_return"] < 0
    assert backtest["10d"]["correct_count"] == 0
    assert backtest["10d"]["reversal_accuracy"] == 0.0


def test_reversal_scoring_rejects_exact_flat_exit():
    df = pd.DataFrame(
        {
            "date": pd.bdate_range("2025-01-02", periods=12),
            "close": [100.0] * 12,
            "ATR14": [10.0] * 12,
            "touched_side": ["Upper"] + [None] * 11,
        }
    )
    decisions = {
        0: _manual_decision(
            _manual_horizon("prediction", "reversal"),
            _manual_horizon("prediction", "reversal"),
        )
    }

    backtest = run_decision_backtest(df, decisions_by_index=decisions)
    prediction = backtest["5d"]["predictions"][0]

    assert prediction["actual_direction"] == "flat"
    assert prediction["is_correct"] is False
    assert prediction["trade_return"] == 0.0
    assert backtest["5d"]["correct_count"] == 0
    assert backtest["5d"]["flat_count"] == 1
    assert backtest["5d"]["reversal_accuracy"] == 0.0


def test_half_atr_move_in_predicted_direction_is_correct():
    df = pd.DataFrame(
        {
            "date": pd.bdate_range("2025-01-02", periods=12),
            "close": [100.0] * 12,
            "ATR14": [2.0] * 12,
            "touched_side": [None] * 12,
        }
    )
    df.loc[0, "touched_side"] = "Upper"
    df.loc[5, "close"] = 101.0
    df.loc[6, "touched_side"] = "Upper"
    df.loc[11, "close"] = 99.0
    decisions = {
        0: _manual_decision(
            _manual_horizon("prediction", "continuation", confidence=0.91),
            _manual_horizon("no_prediction"),
        ),
        6: _manual_decision(
            _manual_horizon("prediction", "reversal", confidence=0.91),
            _manual_horizon("no_prediction"),
        ),
    }

    backtest = run_decision_backtest(df, decisions_by_index=decisions)
    predictions = {item["signal_date"]: item for item in backtest["5d"]["predictions"]}

    assert backtest["5d"]["prediction_count"] == 2
    assert backtest["5d"]["correct_count"] == 2
    assert backtest["5d"]["flat_count"] == 0
    assert predictions["2025-01-02"]["actual_direction"] == "continuation"
    assert predictions["2025-01-10"]["actual_direction"] == "reversal"


def test_intrahorizon_half_atr_target_hit_counts_even_when_exit_close_is_flat():
    df = pd.DataFrame(
        {
            "date": pd.bdate_range("2025-01-02", periods=12),
            "close": [100.0] * 12,
            "high": [100.0, 102.0, 105.0] + [100.0] * 9,
            "low": [100.0] * 12,
            "ATR14": [10.0] * 12,
            "touched_side": ["Upper"] + [None] * 11,
        }
    )
    decisions = {
        0: _manual_decision(
            _manual_horizon("prediction", "continuation", confidence=0.91),
            _manual_horizon("no_prediction"),
        )
    }

    backtest = run_decision_backtest(df, decisions_by_index=decisions)
    prediction = backtest["5d"]["predictions"][0]

    assert prediction["actual_direction"] == "flat"
    assert prediction["predicted_target_hit"] is True
    assert prediction["target_atr_multiple"] == 0.5
    assert prediction["is_correct"] is True
    assert backtest["5d"]["correct_count"] == 1


def test_signal_day_high_low_does_not_count_for_target_hit():
    df = pd.DataFrame(
        {
            "date": pd.bdate_range("2025-01-02", periods=12),
            "close": [100.0] * 12,
            "high": [110.0] + [104.0] * 11,
            "low": [100.0] * 12,
            "ATR14": [10.0] * 12,
            "touched_side": ["Upper"] + [None] * 11,
        }
    )
    decisions = {
        0: _manual_decision(
            _manual_horizon("prediction", "continuation", confidence=0.91),
            _manual_horizon("no_prediction"),
        )
    }

    backtest = run_decision_backtest(df, decisions_by_index=decisions)
    prediction = backtest["5d"]["predictions"][0]

    assert prediction["actual_direction"] == "flat"
    assert prediction["predicted_target_hit"] is False
    assert prediction["continuation_target_hit"] is False
    assert prediction["is_correct"] is False
    assert backtest["5d"]["correct_count"] == 0


def test_lower_band_reversal_veto_blocks_falling_knife_without_exhaustion():
    row = pd.Series(
        {
            "touched_side": "Lower",
            "analysis_side_sign": -1.0,
            "side_ret_5d": 0.0622,
            "side_ret_10d": 0.0477,
            "side_weighted_volume_pressure_5": -0.1807,
            "touch_wick_minus_body": 0.1603,
            "touch_depth_atr": 0.441,
            "consecutive_touch_count": 4.0,
            "touch_reentry_signal": -1.0,
            "side_qqq_ret_5d": 0.0082,
            "side_xlk_ret_5d": -0.0033,
            "band_width_percentile": 0.1822,
            "bandwidth_change_5d": -0.0064,
        }
    )

    assert _reversal_veto_reason(row, 10) == "falling_knife_no_exhaustion"


def test_lower_band_reversal_veto_allows_exhaustion_setup():
    row = pd.Series(
        {
            "touched_side": "Lower",
            "analysis_side_sign": -1.0,
            "side_ret_5d": 0.0672,
            "side_ret_10d": 0.0865,
            "side_weighted_volume_pressure_5": 0.4146,
            "touch_wick_minus_body": -0.1908,
            "touch_depth_atr": 0.4016,
            "consecutive_touch_count": 6.0,
            "touch_reentry_signal": -1.0,
            "side_qqq_ret_5d": -0.006,
            "side_xlk_ret_5d": -0.0264,
            "band_width_percentile": 0.4372,
            "bandwidth_change_5d": 0.0475,
        }
    )

    assert _reversal_veto_reason(row, 10) is None


def test_deployment_quality_gate_quarantines_weak_live_edge():
    gate = _deployment_quality_gate(
        {
            "prediction_count": 2,
            "accuracy": 0.0,
            "reversal_call_count": 2,
            "reversal_accuracy": 0.0,
            "continuation_call_count": 0,
            "continuation_accuracy": None,
        }
    )

    assert gate["deployment_enabled"] is False
    assert gate["status"] == "quarantined"
    assert "weak_deployed_accuracy" in gate["failures"]
    assert "weak_reverse_accuracy" in gate["failures"]


def test_deployment_quality_gate_quarantines_all_filtered_raw_predictions():
    gate = _deployment_quality_gate(
        {
            "prediction_count": 0,
            "deployment_filtered_prediction_count": 39,
            "raw_prediction_count": 39,
            "raw_accuracy": 0.564103,
        }
    )

    assert gate["deployment_enabled"] is False
    assert gate["status"] == "quarantined"
    assert gate["failures"] == ["all_raw_predictions_quarantined"]
    assert gate["raw_prediction_count"] == 39
    assert gate["raw_accuracy"] == 0.564103


def test_deployment_quality_gate_blocks_horizon_prediction_payload():
    decision = _manual_decision(
        _manual_horizon("prediction", "reversal", confidence=0.99),
        _manual_horizon("no_prediction"),
    )
    gates = {
        "5d": {
            "deployment_enabled": False,
            "status": "quarantined",
            "failures": ["weak_reverse_accuracy"],
            "raw_prediction_count": 2,
            "raw_accuracy": 0.0,
            "raw_reverse_accuracy": 0.0,
            "raw_continue_accuracy": None,
        }
    }

    gated = _apply_deployment_quality_gates_to_decision(decision, gates)

    assert gated["horizons"]["5d"]["status"] == "no_prediction"
    assert gated["horizons"]["5d"]["no_prediction_reason"] == "deployment_quality_gate_failed"
    assert gated["horizons"]["5d"]["deployment_quality_gate"]["status"] == "quarantined"
    assert gated["horizons"]["5d"]["blocked_prediction"]["predicted_direction"] == "reversal"


def test_direction_quality_gate_blocks_unseen_signal_family():
    decision = _manual_decision(
        _manual_horizon("prediction", "continuation", confidence=0.99, signal_id="new_continue"),
        _manual_horizon("no_prediction"),
    )
    gates = {
        "5d": {
            "status": "passed",
            "deployment_enabled": True,
            "continuation": {
                "status": "passed",
                "deployment_enabled": True,
                "direction": "continuation",
                "failures": [],
                "raw_prediction_count": 12,
                "raw_correct_count": 12,
                "raw_accuracy": 1.0,
                "raw_wilson_lower_bound": 0.92,
            },
            "reversal": {
                "status": "quarantined",
                "deployment_enabled": False,
                "direction": "reversal",
                "failures": ["insufficient_direction_sample"],
            },
            "signals": {},
        }
    }

    gated = _apply_direction_quality_gates_to_decision(decision, gates)

    assert gated["horizons"]["5d"]["status"] == "no_prediction"
    assert gated["horizons"]["5d"]["no_prediction_reason"] == "direction_quality_gate_failed"
    assert gated["horizons"]["5d"]["deployment_quality_gate"]["failures"] == ["missing_signal_backtest"]
    assert gated["horizons"]["5d"]["blocked_prediction"]["predicted_direction"] == "continuation"


def test_existing_cached_decision_is_gated_before_payload_use():
    df = pd.DataFrame(
        {
            "date": pd.bdate_range("2026-01-02", periods=1),
            "close": [100.0],
            "touched_side": ["Upper"],
            "event_risk_blocked": [False],
        }
    )
    decisions = {
        0: _manual_decision(
            _manual_horizon("prediction", "continuation", confidence=0.99, signal_id="new_continue"),
            _manual_horizon("no_prediction"),
        )
    }
    backtest_1y = {
        "5d": {
            "direction_quality_gate": {
                "continuation": {
                    "status": "passed",
                    "deployment_enabled": True,
                    "direction": "continuation",
                    "failures": [],
                },
                "reversal": {
                    "status": "quarantined",
                    "deployment_enabled": False,
                    "direction": "reversal",
                    "failures": ["insufficient_direction_sample"],
                },
                "signals": {},
            },
            "coverage_repair_policies": [],
        },
        "10d": {
            "direction_quality_gate": {},
            "coverage_repair_policies": [],
        },
    }

    decision = _ensure_decision_for_index(df, decisions, backtest_1y, 0)

    assert decision["horizons"]["5d"]["status"] == "no_prediction"
    assert decision["horizons"]["5d"]["deployment_quality_gate"]["failures"] == ["missing_signal_backtest"]
    assert decision["horizons"]["5d"]["blocked_prediction"]["predicted_direction"] == "continuation"


def test_existing_cached_decision_can_use_prior_hierarchical_evidence_gate():
    dates = pd.bdate_range("2026-01-02", periods=70)
    df = pd.DataFrame(
        {
            "date": dates,
            "close": [100.0] * len(dates),
            "touched_side": [None] * len(dates),
            "event_risk_blocked": [False] * len(dates),
        }
    )
    target_idx = 60
    df.loc[target_idx, "touched_side"] = "Upper"
    raw_predictions = [
        {
            "signal_date": str(dates[i * 3].date()),
            "outcome_date": str(dates[i * 3 + 5].date()),
            "horizon_days": 5,
            "touched_side": "Upper",
            "predicted_direction": "continuation",
            "actual_direction": "continuation",
            "is_correct": True,
            "confidence_score": 70,
            "signal_model_id": f"unique_continue_{i}",
            "signal_tier": "regime",
            "trade_return": 0.02,
            "trade_return_atr": 0.5,
        }
        for i in range(18)
    ]
    decisions = {
        target_idx: _manual_decision(
            _manual_horizon("prediction", "continuation", confidence=0.70, tier="regime", signal_id="new_continue"),
            _manual_horizon("no_prediction"),
        )
    }
    backtest_1y = {
        "5d": {
            "_raw_predictions_for_gate": raw_predictions,
            "direction_quality_gate": {
                "continuation": {
                    "status": "passed",
                    "deployment_enabled": True,
                    "direction": "continuation",
                    "failures": [],
                },
                "reversal": {
                    "status": "quarantined",
                    "deployment_enabled": False,
                    "direction": "reversal",
                    "failures": ["insufficient_direction_sample"],
                },
                "signals": {},
            },
            "coverage_repair_policies": [],
        },
        "10d": {
            "direction_quality_gate": {},
            "coverage_repair_policies": [],
        },
    }

    decision = _ensure_decision_for_index(df, decisions, backtest_1y, target_idx)

    assert decision["horizons"]["5d"]["status"] == "prediction"
    gate = decision["horizons"]["5d"]["deployment_quality_gate"]
    assert gate["deployment_enabled"] is True
    assert gate["evidence_scope"] == "tier_side_confidence"
    assert gate["raw_prediction_count"] == 18


def test_finalize_reports_prior_evidence_deployment_metrics_with_raw_diagnostics():
    rows = 172
    df = pd.DataFrame(
        {
            "date": pd.bdate_range("2025-01-02", periods=rows),
            "close": [100.0] * rows,
            "touched_side": [None] * rows,
        }
    )
    touch_indices = [idx * 6 for idx in range(26)]
    actual_reversal = set(touch_indices) - {6}
    for idx in touch_indices:
        df.loc[idx, "touched_side"] = "Upper"
        df.loc[idx + 5, "close"] = 90.0 if idx in actual_reversal else 110.0

    decisions = {
        0: _manual_decision(
            _manual_horizon("prediction", "reversal", tier="core", signal_id="bad_reversal"),
            _manual_horizon("no_prediction"),
        ),
        6: _manual_decision(
            _manual_horizon("prediction", "reversal", tier="core", signal_id="bad_reversal"),
            _manual_horizon("no_prediction"),
        ),
    }
    for idx in touch_indices[2:]:
        decisions[idx] = _manual_decision(
            _manual_horizon("prediction", "reversal", tier="regime", signal_id="good_reversal"),
            _manual_horizon("no_prediction"),
        )

    _, final_backtest = _finalize_deployment_quality(df, decisions)
    five_day = final_backtest["5d"]
    signals = five_day["direction_quality_gate"]["signals"]

    assert five_day["backtest_policy"] == "walk_forward_prior_evidence_deployment_gate"
    assert five_day["prediction_count"] == 19
    assert five_day["accuracy"] == 1.0
    assert five_day["coverage"] == pytest.approx(19 / 26)
    assert five_day["reversal_accuracy"] == 1.0
    assert five_day["raw_prediction_count"] == 26
    assert five_day["raw_accuracy"] == pytest.approx(25 / 26)
    assert five_day["raw_reverse_accuracy"] == pytest.approx(25 / 26)
    assert five_day["raw_coverage"] == 1.0
    assert five_day["raw_backtest_policy"] == "raw_walk_forward_no_in_sample_gate_or_repair"
    assert five_day["signal_tier_counts"] == {"regime": 19}
    assert signals["reversal:good_reversal"]["status"] == "passed"
    assert signals["reversal:bad_reversal"]["deployment_enabled"] is False


def test_prior_evidence_deployment_can_use_predeclared_broader_family_when_exact_is_under_sampled():
    actual_by_index = {idx * 6: "continuation" for idx in range(25)}
    df = _coverage_gate_frame(actual_by_index)
    decisions = {
        idx: _manual_decision(
            _manual_horizon("prediction", "continuation", tier="regime", signal_id=f"unique_continue_{idx}"),
            _manual_horizon("no_prediction"),
        )
        for idx in actual_by_index
    }

    _, final_backtest = _finalize_deployment_quality(df, decisions)
    five_day = final_backtest["5d"]

    assert five_day["raw_prediction_count"] == 25
    assert five_day["prediction_count"] == 20
    assert five_day["accuracy"] == 1.0
    assert five_day["coverage"] == 0.8
    assert {
        prediction["deployment_quality_gate"]["evidence_scope"]
        for prediction in five_day["predictions"]
    } == {"tier_side_confidence"}


def test_prior_evidence_deployment_can_use_completed_rows_before_report_window():
    rows = 420
    dates = pd.bdate_range("2024-01-02", periods=rows)
    df = pd.DataFrame(
        {
            "date": dates,
            "close": [100.0] * rows,
            "touched_side": [None] * rows,
        }
    )
    pre_report_indices = [idx * 6 for idx in range(20)]
    report_idx = 300
    all_touch_indices = [*pre_report_indices, report_idx]
    for idx in all_touch_indices:
        df.loc[idx, "touched_side"] = "Upper"
        df.loc[idx, "close"] = 100.0
        df.loc[idx + 5, "close"] = 110.0

    decisions = {
        idx: _manual_decision(
            _manual_horizon("prediction", "continuation", tier="regime", signal_id="older_exact_continue"),
            _manual_horizon("no_prediction"),
        )
        for idx in all_touch_indices
    }

    _, final_backtest = _finalize_deployment_quality(df, decisions)
    five_day = final_backtest["5d"]

    assert pd.Timestamp(five_day["period_start"]) > dates[pre_report_indices[-1]]
    assert five_day["raw_prediction_count"] == 1
    assert five_day["deployment_evidence_prediction_count"] == 21
    assert five_day["prediction_count"] == 1
    assert five_day["accuracy"] == 1.0
    assert five_day["predictions"][0]["signal_date"] == str(dates[report_idx].date())
    assert five_day["predictions"][0]["deployment_quality_gate"]["evidence_scope"] == "exact_signal_side"
    assert five_day["predictions"][0]["deployment_quality_gate"]["raw_prediction_count"] == 20


def test_prior_evidence_deployment_does_not_let_broad_family_override_bad_exact_history():
    actual_by_index = {
        0: "continuation",
        6: "continuation",
        12: "continuation",
        18: "reversal",
        24: "reversal",
        30: "continuation",
        36: "continuation",
        42: "continuation",
        48: "continuation",
        54: "continuation",
        60: "continuation",
        66: "continuation",
    }
    df = _coverage_gate_frame(actual_by_index)
    bad_exact_indices = [0, 6, 12, 18, 24, 66]
    decisions = {}
    for idx in actual_by_index:
        signal_id = "bad_exact_continue" if idx in bad_exact_indices else f"good_family_{idx}"
        decisions[idx] = _manual_decision(
            _manual_horizon("prediction", "continuation", tier="regime", signal_id=signal_id),
            _manual_horizon("no_prediction"),
        )

    _, final_backtest = _finalize_deployment_quality(df, decisions)
    five_day = final_backtest["5d"]

    assert five_day["raw_prediction_count"] == 12
    assert all(
        prediction.get("signal_model_id") != "bad_exact_continue"
        for prediction in five_day["predictions"]
    )


def test_signal_gate_requires_own_minimum_sample_even_when_direction_passes():
    actual_by_index = {idx * 6: "continuation" for idx in range(18)}
    df = _coverage_gate_frame(actual_by_index)
    decisions = {}
    for ordinal, idx in enumerate(actual_by_index):
        decisions[idx] = _manual_decision(
            _manual_horizon(
                "prediction",
                "continuation",
                signal_id="core_continue" if ordinal < 16 else "tiny_continue",
            ),
            _manual_horizon("no_prediction"),
        )

    _, final_backtest = _finalize_deployment_quality(df, decisions)
    gates = final_backtest["5d"]["direction_quality_gate"]
    signals = gates["signals"]

    assert gates["continuation"]["status"] == "passed"
    assert signals["continuation:core_continue"]["deployment_enabled"] is True
    assert signals["continuation:tiny_continue"]["deployment_enabled"] is False
    assert "insufficient_signal_sample" in signals["continuation:tiny_continue"]["failures"]


def test_finalize_does_not_expand_coverage_on_the_same_scored_window():
    actual_by_index = {
        0: "continuation",
        6: "continuation",
        12: "continuation",
        18: "reversal",
        24: "continuation",
        30: "continuation",
        36: "continuation",
        42: "continuation",
        48: "continuation",
        54: "continuation",
    }
    df = _coverage_gate_frame(actual_by_index)
    decisions = {
        0: _manual_decision(
            _manual_horizon("prediction", "continuation", tier="core", signal_id="core_continue"),
            _manual_horizon("no_prediction"),
        ),
        6: _manual_decision(
            _manual_horizon("prediction", "continuation", tier="core", signal_id="core_continue"),
            _manual_horizon("no_prediction"),
        ),
        12: _manual_decision(
            _manual_horizon("prediction", "continuation", tier="core", signal_id="core_continue"),
            _manual_horizon("no_prediction"),
        ),
        18: _manual_decision(
            _manual_horizon("prediction", "reversal", signal_id="isolated_reversal"),
            _manual_horizon("no_prediction"),
        ),
        24: _manual_decision(
            _manual_horizon("prediction", "reversal", signal_id="weak_reversal"),
            _manual_horizon("no_prediction"),
        ),
        30: _manual_decision(
            _manual_horizon("prediction", "continuation", signal_id="extra_continue"),
            _manual_horizon("no_prediction"),
        ),
        36: _manual_decision(
            _manual_horizon("prediction", "continuation", signal_id="extra_continue"),
            _manual_horizon("no_prediction"),
        ),
    }

    _, final_backtest = _finalize_deployment_quality(df, decisions)
    five_day = final_backtest["5d"]
    signals = five_day["direction_quality_gate"]["signals"]

    assert "coverage_target" not in five_day
    assert five_day["coverage_policy"] == "walk_forward_prior_evidence_deployment_gate"
    assert five_day["backtest_policy"] == "walk_forward_prior_evidence_deployment_gate"
    assert five_day["raw_backtest_policy"] == "raw_walk_forward_no_in_sample_gate_or_repair"
    assert five_day["coverage"] == 0.0
    assert five_day["max_safe_coverage"] == 0.0
    assert five_day["prediction_count"] == 0
    assert five_day["max_safe_prediction_count"] == 0
    assert five_day["accuracy"] is None
    assert five_day["continuation_accuracy"] is None
    assert five_day["reversal_accuracy"] is None
    assert five_day["raw_coverage"] == 0.7
    assert five_day["raw_accuracy"] == pytest.approx(6 / 7)
    assert five_day["coverage_expansion_signal_count"] == 0
    assert five_day["coverage_repair_prediction_count"] == 0
    assert signals["reversal:isolated_reversal"]["status"] == "quarantined"
    assert signals["reversal:isolated_reversal"]["deployment_enabled"] is False
    assert signals["continuation:extra_continue"]["status"] == "quarantined"
    assert signals["continuation:extra_continue"]["deployment_enabled"] is False
    assert "insufficient_signal_sample" in signals["continuation:extra_continue"]["failures"]
    assert signals["reversal:weak_reversal"]["deployment_enabled"] is False


def test_finalize_keeps_raw_coverage_when_accuracy_drop_would_have_been_in_sample_selected():
    actual_by_index = {
        0: "continuation",
        6: "continuation",
        12: "continuation",
        18: "continuation",
        24: "reversal",
        30: "continuation",
        36: "continuation",
        42: "continuation",
        48: "continuation",
        54: "continuation",
    }
    df = _coverage_gate_frame(actual_by_index)
    decisions = {
        0: _manual_decision(
            _manual_horizon("prediction", "continuation", tier="core", signal_id="core_continue"),
            _manual_horizon("no_prediction"),
        ),
        6: _manual_decision(
            _manual_horizon("prediction", "continuation", tier="core", signal_id="core_continue"),
            _manual_horizon("no_prediction"),
        ),
        12: _manual_decision(
            _manual_horizon("prediction", "continuation", tier="core", signal_id="core_continue"),
            _manual_horizon("no_prediction"),
        ),
        18: _manual_decision(
            _manual_horizon("prediction", "continuation", signal_id="weak_continue"),
            _manual_horizon("no_prediction"),
        ),
        24: _manual_decision(
            _manual_horizon("prediction", "continuation", signal_id="weak_continue"),
            _manual_horizon("no_prediction"),
        ),
    }

    _, final_backtest = _finalize_deployment_quality(df, decisions)
    five_day = final_backtest["5d"]
    weak_signal = five_day["direction_quality_gate"]["signals"]["continuation:weak_continue"]

    assert "coverage_target" not in five_day
    assert five_day["coverage_policy"] == "walk_forward_prior_evidence_deployment_gate"
    assert five_day["backtest_policy"] == "walk_forward_prior_evidence_deployment_gate"
    assert five_day["raw_backtest_policy"] == "raw_walk_forward_no_in_sample_gate_or_repair"
    assert five_day["coverage"] == 0.0
    assert five_day["max_safe_coverage"] == 0.0
    assert five_day["prediction_count"] == 0
    assert five_day["max_safe_prediction_count"] == 0
    assert five_day["accuracy"] is None
    assert five_day["raw_coverage"] == 0.5
    assert five_day["raw_prediction_count"] == 5
    assert five_day["raw_accuracy"] == 0.8
    assert five_day["coverage_expansion_signal_count"] == 0
    assert five_day["coverage_repair_prediction_count"] == 0
    assert weak_signal["deployment_enabled"] is False
    assert weak_signal.get("coverage_expansion") is None


def test_coverage_repair_uses_blocked_direction_as_a_separate_regime():
    actual_by_index = {
        0: "continuation",
        6: "continuation",
        12: "continuation",
        18: "reversal",
        24: "reversal",
        30: "reversal",
    }
    df = _coverage_gate_frame(actual_by_index)
    decisions = {
        idx: _manual_decision(
            _blocked_manual_horizon(actual),
            _manual_horizon("no_prediction"),
        )
        for idx, actual in actual_by_index.items()
    }

    candidates = _coverage_repair_candidates(df, decisions, 5)
    policies = [candidate["policy"] for candidate in candidates]

    intent_policies = [
        policy
        for policy in policies
        if "blocked_direction" in policy["fields"]
    ]
    assert intent_policies
    assert {
        (policy["direction"], policy["attrs"]["blocked_direction"])
        for policy in intent_policies
    } >= {
        ("continuation", "continuation"),
        ("reversal", "reversal"),
    }
    assert not any(
        policy["scope"] == "side_only"
        for policy in policies
    )


def test_coverage_repair_confidence_is_calibrated_below_perfect_small_sample_precision():
    actual_by_index = {
        0: "continuation",
        6: "continuation",
        12: "continuation",
    }
    df = _coverage_gate_frame(actual_by_index)
    decisions = {
        idx: _manual_decision(
            _blocked_manual_horizon("continuation"),
            _manual_horizon("no_prediction"),
        )
        for idx in actual_by_index
    }

    candidates = _coverage_repair_candidates(df, decisions, 5)
    policy = max(
        (candidate["policy"] for candidate in candidates if "blocked_direction" in candidate["policy"]["fields"]),
        key=lambda item: item["score"],
    )
    horizon = _coverage_repair_horizon(df, 0, 5, policy)

    assert policy["precision"] == 1.0
    assert policy["calibrated_probability"] < policy["precision"]
    assert horizon["confidence_score"] == round(policy["calibrated_probability"] * 100)
    assert horizon["continuation_probability"] == pytest.approx(policy["calibrated_probability"])


def test_coverage_repair_horizon_exposes_similar_past_cases():
    actual_by_index = {
        0: "reversal",
        6: "reversal",
        12: "reversal",
        18: "reversal",
        24: "reversal",
        30: "reversal",
        36: "continuation",
    }
    df = _coverage_gate_frame(actual_by_index)
    decisions = {
        idx: _manual_decision(
            _blocked_manual_horizon("reversal"),
            _manual_horizon("no_prediction"),
        )
        for idx in actual_by_index
    }

    candidates = _coverage_repair_candidates(df, decisions, 5)
    policy = max(
        (candidate["policy"] for candidate in candidates if candidate["policy"]["direction"] == "reversal"),
        key=lambda item: item["score"],
    )
    horizon = _coverage_repair_horizon(df, 0, 5, policy)

    assert policy["match_count"] == 7
    assert policy["neighbors"]
    assert horizon["key_reasons"]
    assert horizon["key_reasons"][0]["feature"] == "Selective Coverage Repair"
    assert horizon["playbook"]["neighbors"]
    assert horizon["similar_past_cases"]
    case = horizon["similar_past_cases"][0]
    assert case["signal_date"]
    assert case["outcome_date"]
    assert case["predicted_direction"] == "reversal"
    assert case["actual_direction"] in {"reversal", "continuation"}
    assert case["is_correct"] in {True, False}
    assert "trade_return" in case
    assert "_repair_policy" not in case


def test_coverage_selector_does_not_keep_redundant_greedy_policy_candidates():
    duplicate_row = {
        "_repair_row_key": ("2026-01-02", 5),
        "predicted_direction": "continuation",
        "is_correct": True,
        "trade_return": 0.01,
        "trade_return_atr": 0.20,
    }
    candidates = [
        {
            "key": f"duplicate_{idx}",
            "rows": [duplicate_row.copy()],
            "metrics": _prediction_book_metrics([duplicate_row]),
            "gate": {"direction": "continuation"},
            "policy": {"id": f"duplicate_{idx}", "direction": "continuation"},
        }
        for idx in range(19)
    ]

    selected = _select_max_safe_coverage_candidates([], candidates)

    assert len(selected) == 1


def test_coverage_expansion_accepts_more_calls_when_wilson_confidence_improves():
    current_rows = [
        {"predicted_direction": "continuation", "is_correct": True}
        for _ in range(5)
    ]
    trial_rows = current_rows + [
        {"predicted_direction": "continuation", "is_correct": True}
        for _ in range(25)
    ] + [
        {"predicted_direction": "continuation", "is_correct": False}
        for _ in range(2)
    ]

    current_metrics = _prediction_book_metrics(current_rows)
    trial_metrics = _prediction_book_metrics(trial_rows)

    assert trial_metrics["accuracy"] < current_metrics["accuracy"]
    assert _prediction_book_preserves_accuracy(current_metrics, trial_metrics) is True


def test_coverage_expansion_rejects_more_calls_when_accuracy_budget_is_not_preserved():
    current_rows = [
        {"predicted_direction": "continuation", "is_correct": True}
        for _ in range(8)
    ]
    trial_rows = current_rows + [
        {"predicted_direction": "continuation", "is_correct": False}
    ]

    assert _prediction_book_preserves_accuracy(
        _prediction_book_metrics(current_rows),
        _prediction_book_metrics(trial_rows),
    ) is False


def test_expanding_percentiles_do_not_use_future_rows():
    df = _base_frame(120)
    baseline = _prepare_feature_frame(df, symbol="TEST", earnings_dates=set())
    modified = df.copy()
    target_idx = 60
    modified.loc[target_idx + 1 :, "BB_upper"] = modified.loc[target_idx + 1 :, "BB_upper"] * 8
    modified.loc[target_idx + 1 :, "BB_lower"] = modified.loc[target_idx + 1 :, "BB_lower"] * 0.2
    changed = _prepare_feature_frame(modified, symbol="TEST", earnings_dates=set())

    assert changed.loc[target_idx, "band_width_percentile"] == pytest.approx(
        baseline.loc[target_idx, "band_width_percentile"]
    )
    assert changed.loc[target_idx, "band_width_zscore_60"] == pytest.approx(
        baseline.loc[target_idx, "band_width_zscore_60"]
    )
    assert changed.loc[target_idx, "squeeze_rank_120"] == pytest.approx(
        baseline.loc[target_idx, "squeeze_rank_120"]
    )


def test_model_feature_inputs_do_not_change_when_future_rows_change():
    target_idx = 100
    df = _base_frame(180)
    context_frames = {
        "QQQ": _base_frame(180),
        "XLK": _base_frame(180),
    }

    baseline = _prepare_feature_frame(
        df,
        symbol="TEST",
        earnings_dates=set(),
        context_frames=context_frames,
    )
    target_date = pd.Timestamp(baseline.loc[target_idx, "date"]).normalize()

    def distort_future_rows(frame: pd.DataFrame) -> pd.DataFrame:
        changed = frame.copy()
        future_mask = pd.to_datetime(changed["date"], errors="coerce").dt.normalize() > target_date
        for col in ("open", "high", "low", "close", "BB_upper", "BB_middle"):
            if col in changed.columns:
                changed.loc[future_mask, col] = changed.loc[future_mask, col] * 3.7 + 11.0
        if "BB_lower" in changed.columns:
            changed.loc[future_mask, "BB_lower"] = changed.loc[future_mask, "BB_lower"] * 0.2
        if "volume" in changed.columns:
            changed.loc[future_mask, "volume"] = changed.loc[future_mask, "volume"] * 7.0 + 12345.0
        return changed

    changed = _prepare_feature_frame(
        distort_future_rows(df),
        symbol="TEST",
        earnings_dates=set(),
        context_frames={symbol: distort_future_rows(frame) for symbol, frame in context_frames.items()},
    )

    audited_columns = [
        col
        for col in (*_MODEL_FEATURES, "touched_side", "analysis_side", "event_risk_blocked")
        if col in baseline.columns
    ]
    for col in audited_columns:
        before = baseline.loc[:target_idx, col]
        after = changed.loc[:target_idx, col]
        if before.dtype == object or after.dtype == object:
            assert before.fillna("__NA__").astype(str).tolist() == after.fillna("__NA__").astype(str).tolist(), col
            continue
        before_values = pd.to_numeric(before, errors="coerce").to_numpy(dtype=float)
        after_values = pd.to_numeric(after, errors="coerce").to_numpy(dtype=float)
        assert np.allclose(before_values, after_values, equal_nan=True, rtol=1e-12, atol=1e-12), col


def test_model_feature_schema_does_not_include_outcome_columns():
    forbidden_fragments = ("future", "outcome", "actual", "label", "forward")

    for feature in _MODEL_FEATURES:
        assert not any(fragment in feature.lower() for fragment in forbidden_fragments), feature


def test_feature_engineering_does_not_use_negative_time_shifts():
    features_path = Path(tee.__file__).parent / "features.py"
    text = features_path.read_text()

    for forbidden in (".shift(-", ".pct_change(-"):
        assert forbidden not in text


def test_expanded_model_feature_frame_contains_ta_inputs():
    feature_df = _prepare_feature_frame(_base_frame(180), symbol="TEST", earnings_dates=set())

    for feature in _MODEL_FEATURES:
        assert feature in feature_df.columns

    advanced_columns = [
        "EMA10",
        "KAMA20",
        "PPO",
        "STOCH_k",
        "STOCHRSI_k",
        "ADOSC",
        "NATR14",
        "donchian55_position",
        "side_ema20_slope_5",
        "side_high_low_breakout_20",
    ]
    for column in advanced_columns:
        assert feature_df[column].notna().any()


def test_earnings_blackout_blocks_mu_entry_when_earnings_falls_inside_prediction_horizon():
    frame = _base_frame(420)
    feature_df = _prepare_feature_frame(
        frame,
        symbol="MU",
        earnings_dates={pd.Timestamp("2026-03-18")},
    )

    dates = pd.DatetimeIndex(pd.to_datetime(feature_df["date"])).normalize()
    blocked_idx = int(np.flatnonzero(dates == pd.Timestamp("2026-03-16"))[0])
    safe_idx = int(np.flatnonzero(dates == pd.Timestamp("2026-03-02"))[0])

    assert bool(feature_df.iloc[blocked_idx]["event_risk_blocked"]) is True
    assert bool(feature_df.iloc[safe_idx]["event_risk_blocked"]) is False
    assert feature_df.iloc[blocked_idx]["event_risk_event_date"] == "2026-03-18"
    assert feature_df.iloc[blocked_idx]["event_risk_reason"] == "earnings_within_prediction_window"

    decision = evaluate_row_decision(
        feature_df.iloc[blocked_idx],
        feature_df=feature_df,
        row_index=blocked_idx,
    )

    assert decision["event_risk_blocked"] is True
    assert decision["event_risk"]["event_date"] == "2026-03-18"
    assert decision["event_risk"]["blocked_horizons"] == ["5d", "10d"]
    assert decision["horizons"]["5d"]["status"] == "no_prediction"
    assert decision["horizons"]["5d"]["no_prediction_reason"] == "event_risk"
    assert decision["horizons"]["5d"]["event_risk"]["reason"] == "earnings_within_prediction_window"


def test_earnings_blackout_is_horizon_specific_to_preserve_safe_coverage():
    frame = _base_frame(420)
    target_date = pd.Timestamp("2026-03-09")
    target_pos = int(np.flatnonzero(pd.DatetimeIndex(frame["date"]).normalize() == target_date)[0])
    target_label = frame.index[target_pos]
    lower = float(frame.loc[target_label, "BB_lower"])
    frame.loc[target_label, "low"] = lower - 1.0
    frame.loc[target_label, "close"] = lower + 0.08
    frame.loc[target_label, "open"] = lower + 0.10
    frame.loc[target_label, "high"] = lower + 0.7

    feature_df = _prepare_feature_frame(
        frame,
        symbol="MU",
        earnings_dates={pd.Timestamp("2026-03-18")},
    )
    target_idx = int(np.flatnonzero(pd.DatetimeIndex(feature_df["date"]).normalize() == target_date)[0])

    decision = evaluate_row_decision(
        feature_df.iloc[target_idx],
        feature_df=feature_df,
        row_index=target_idx,
    )

    assert decision["event_risk_blocked"] is True
    assert decision["event_risk"]["blocked_horizons"] == ["10d"]
    assert decision["horizons"]["5d"]["event_risk"]["blocked"] is False
    assert decision["horizons"]["5d"].get("no_prediction_reason") != "event_risk"
    assert decision["horizons"]["10d"]["status"] == "no_prediction"
    assert decision["horizons"]["10d"]["no_prediction_reason"] == "event_risk"
    assert decision["horizons"]["10d"]["event_risk"]["blocked"] is True


def test_yfinance_earnings_lookup_noise_is_suppressed(capsys):
    logger = logging.getLogger("yfinance")

    with _quiet_yfinance_earnings_lookup():
        print("HTTP Error 401: invalid crumb")
        sys.stderr.write("HTTP Error 401: invalid crumb\n")
        logger.error("HTTP Error 401: invalid crumb")

    captured = capsys.readouterr()
    assert "HTTP Error 401" not in captured.out
    assert "HTTP Error 401" not in captured.err


def test_as_of_date_exact_trading_day_uses_same_date():
    df = _force_lower_touch(_base_frame())
    target_date = pd.Timestamp(df["date"].iloc[-5]).strftime("%Y-%m-%d")

    payload = build_entry_decision_from_frame("TEST", df, as_of_date=target_date, earnings_dates=set())

    assert payload["requested_as_of_date"] == target_date
    assert payload["as_of_date"] == target_date
    assert payload["date_was_snapped"] is False


def test_historical_as_of_uses_cached_point_in_time_context_without_rebuild(monkeypatch):
    frame = _minimal_feature_context_frame()
    may6_prediction = _manual_decision(
        _manual_horizon("prediction", "reversal", confidence=0.91),
        _manual_horizon("prediction", "reversal", confidence=0.93),
    )

    def fail_point_in_time_rebuild(*_args, **_kwargs):
        pytest.fail("Selected-date payloads must reuse the cached point-in-time context when available.")

    monkeypatch.setattr(tee_payload, "_finalized_point_in_time_context", fail_point_in_time_rebuild)
    context = {
        "symbol": "GOOGL",
        "feature_df": frame,
        "decisions_by_index": {1: may6_prediction},
        "backtest_1y": _minimal_backtest_payload(),
        "_point_in_time_cache": {
            1: {
                "decisions_by_index": {1: may6_prediction},
                "backtest_1y": _minimal_backtest_payload(),
            }
        },
    }

    payload = build_entry_decision_from_context(context, as_of_date="2026-05-06")

    assert payload["as_of_date"] == "2026-05-06"
    assert payload["chart_data"][-1]["date"] == "2026-05-06"
    assert payload["horizons"]["10d"]["status"] == "prediction"
    assert payload["horizons"]["10d"]["predicted_direction"] == "short"
    assert payload["backtest_1y"]["10d"]["open_predictions"][0]["signal_date"] == "2026-05-06"
    assert payload["backtest_1y"]["10d"]["open_predictions"][0]["predicted_direction"] == "short"


def test_historical_as_of_payload_backtest_is_cut_to_selected_date():
    df = _base_frame(95)
    target_date = pd.Timestamp(df["date"].iloc[70]).strftime("%Y-%m-%d")

    payload = build_entry_decision_from_frame("TEST", df, as_of_date=target_date, earnings_dates=set())

    assert payload["as_of_date"] == target_date
    assert payload["chart_data"][-1]["date"] == target_date
    for result in payload["backtest_1y"].values():
        if not isinstance(result, dict):
            continue
        assert pd.Timestamp(result["period_end"]) <= pd.Timestamp(target_date)
        for key in ("predictions", "recent_predictions", "raw_recent_predictions"):
            for prediction in result.get(key, []) or []:
                assert pd.Timestamp(prediction["signal_date"]) <= pd.Timestamp(target_date)


def test_entry_decision_model_logic_has_no_target_symbol_specific_rules():
    model_dir = Path(tee.__file__).parent
    model_files = ("adaptive.py", "empirical.py", "model.py", "settings.py", "quality.py", "deployment.py")
    text = "\n".join((model_dir / filename).read_text() for filename in model_files)

    screenshot_symbols = (
        "NVDA",
        "GOOGL",
        "GOOG",
        "AAPL",
        "MSFT",
        "AMZN",
        "AVGO",
        "TSLA",
        "META",
        "BRK.B",
        "BRK-B",
        "WMT",
        "LLY",
        "MU",
        "JPM",
        "AMD",
        "XOM",
        "V",
    )
    for symbol in screenshot_symbols:
        assert f'"{symbol}"' not in text
        assert f"'{symbol}'" not in text


def test_latest_payload_keeps_prior_open_prediction_marker(monkeypatch):
    frame = _minimal_feature_context_frame()
    may6_prediction = _manual_decision(
        _manual_horizon("prediction", "reversal", confidence=0.91),
        _manual_horizon("prediction", "reversal", confidence=0.93),
    )
    latest_no_touch = {
        "touched_side": None,
        "setup_type": "no_band_setup",
        "event_risk_blocked": False,
        "horizons": {
            "5d": _manual_horizon("no_prediction"),
            "10d": _manual_horizon("no_prediction"),
        },
        "top_reasons": [],
    }
    monkeypatch.setattr(
        tee,
        "_point_in_time_decision_for_index",
        lambda *_args, **_kwargs: pytest.fail("Open prediction markers should reuse cached decisions."),
    )
    context = {
        "symbol": "GOOGL",
        "feature_df": frame,
        "decisions_by_index": {1: may6_prediction, 2: latest_no_touch},
        "backtest_1y": _minimal_backtest_payload(),
    }

    payload = build_entry_decision_from_context(context, as_of_date="2026-05-07")

    open_predictions = payload["backtest_1y"]["10d"]["open_predictions"]
    assert payload["as_of_date"] == "2026-05-07"
    assert len(open_predictions) == 1
    assert open_predictions[0]["signal_date"] == "2026-05-06"
    assert open_predictions[0]["current_date"] == "2026-05-07"
    assert open_predictions[0]["status"] == "open"
    assert open_predictions[0]["predicted_direction"] == "short"
    assert open_predictions[0]["elapsed_sessions"] == 1
    assert open_predictions[0]["remaining_sessions"] == 9
    assert open_predictions[0]["current_close"] == 102.5
    assert open_predictions[0]["interim_status"] == "flat"


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


def test_entry_context_metadata_versions_data_end_and_quality():
    df = _force_lower_touch(_base_frame(140))

    context = build_entry_decision_context_from_frame("TEST", df, earnings_dates=set())
    payload = build_entry_decision_from_context(context)

    meta = context["meta"]
    assert meta["model_version"] == entry_decision_model_version()
    assert meta["feature_schema_version"] == entry_decision_feature_schema_version()
    assert meta["price_data_end_date"] == pd.Timestamp(df.iloc[-1]["date"]).strftime("%Y-%m-%d")
    assert meta["trained_through_date"] == meta["price_data_end_date"]
    assert meta["context_key"]
    assert meta["quality"]["status"] in {"passed", "quarantined", "idle", "unknown"}

    assert payload["meta"]["full_decision_preloaded"] is True
    assert payload["meta"]["context"]["context_key"] == meta["context_key"]
    assert payload["meta"]["freshness"]["status"] in {"fresh", "stale", "expired", "unknown"}
    assert payload["meta"]["quality"] == meta["quality"]


def test_entry_context_can_be_cut_to_latest_completed_price_date():
    df = _force_lower_touch(_base_frame(140))
    cutoff_date = pd.Timestamp(df["date"].iloc[-3]).strftime("%Y-%m-%d")

    context = build_entry_decision_context_from_frame(
        "TEST",
        df,
        earnings_dates=set(),
        price_data_cutoff_date=cutoff_date,
    )

    assert context["meta"]["price_data_end_date"] == cutoff_date
    assert pd.Timestamp(context["feature_df"]["date"].iloc[-1]).strftime("%Y-%m-%d") == cutoff_date


def test_entry_context_cutoff_requires_latest_completed_price_date():
    df = _force_lower_touch(_base_frame(140))
    missing_cutoff = (pd.Timestamp(df["date"].iloc[-1]) + pd.offsets.BDay(1)).strftime("%Y-%m-%d")

    with pytest.raises(ValueError, match="Latest required close is not available yet"):
        build_entry_decision_context_from_frame(
            "TEST",
            df,
            earnings_dates=set(),
            price_data_cutoff_date=missing_cutoff,
        )


def test_entry_context_freshness_detects_fresh_stale_and_expired(monkeypatch):
    chicago = pytz.timezone("America/Chicago")
    base_meta = {
        "model_version": entry_decision_model_version(),
        "feature_schema_version": entry_decision_feature_schema_version(),
        "price_data_end_date": "2026-04-15",
    }

    during_apr16_session = chicago.localize(datetime(2026, 4, 16, 10, 0))
    after_apr16_close = chicago.localize(datetime(2026, 4, 16, 15, 45))
    after_apr17_close = chicago.localize(datetime(2026, 4, 17, 15, 45))

    fresh = evaluate_entry_context_freshness(base_meta, now=during_apr16_session)
    assert fresh["status"] == "fresh"
    assert fresh["serving_allowed"] is True
    assert fresh["latest_required_price_date"] == "2026-04-15"

    stale = evaluate_entry_context_freshness(base_meta, now=after_apr16_close)
    assert stale["status"] == "stale"
    assert stale["serving_allowed"] is True
    assert stale["stale_sessions"] == 1

    expired = evaluate_entry_context_freshness(base_meta, now=after_apr17_close)
    assert expired["status"] == "expired"
    assert expired["serving_allowed"] is False
    assert expired["stale_sessions"] == 2

    monkeypatch.setenv("ENTRY_DECISION_CONTEXT_MAX_STALE_SESSIONS", "0")
    stricter = evaluate_entry_context_freshness(base_meta, now=after_apr16_close)
    assert stricter["status"] == "expired"
    assert stricter["serving_allowed"] is False


def test_entry_context_freshness_expires_when_model_or_schema_changes():
    now = pytz.timezone("America/Chicago").localize(datetime(2026, 4, 16, 10, 0))
    good_meta = {
        "model_version": entry_decision_model_version(),
        "feature_schema_version": entry_decision_feature_schema_version(),
        "price_data_end_date": "2026-04-15",
    }

    model_changed = evaluate_entry_context_freshness(
        {**good_meta, "model_version": "entry-old"},
        now=now,
    )
    schema_changed = evaluate_entry_context_freshness(
        {**good_meta, "feature_schema_version": "features-old"},
        now=now,
    )

    assert model_changed["status"] == "expired"
    assert model_changed["reason"] == "model_version_changed"
    assert model_changed["serving_allowed"] is False
    assert schema_changed["status"] == "expired"
    assert schema_changed["reason"] == "feature_schema_changed"
    assert schema_changed["serving_allowed"] is False


def test_entry_context_lru_cache_day_tracks_latest_required_price_date(monkeypatch):
    monkeypatch.setitem(tee._entry_cache_day.__globals__, "latest_required_price_date", lambda: "2026-04-16")

    assert tee._entry_cache_day() == "2026-04-16"
