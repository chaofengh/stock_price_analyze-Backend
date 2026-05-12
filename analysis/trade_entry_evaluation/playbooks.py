from __future__ import annotations

from .settings import *
from .features import *
from .model import *

_PLAYBOOKS = [
    {
        "id": "lower_falling_knife_continue",
        "name": "Lower Band Falling Knife",
        "direction": "continuation",
    },
    {
        "id": "upper_breakout_continue",
        "name": "Upper Band Breakout",
        "direction": "continuation",
    },
    {
        "id": "band_walk_continue",
        "name": "Band Walk",
        "direction": "continuation",
    },
    {
        "id": "context_trend_continue",
        "name": "Market-Aligned Trend",
        "direction": "continuation",
    },
    {
        "id": "reentry_reversal",
        "name": "Band Re-Entry Reversal",
        "direction": "reversal",
    },
    {
        "id": "capitulation_reversal",
        "name": "Capitulation Reversal",
        "direction": "reversal",
    },
    {
        "id": "blowoff_reversal",
        "name": "Blow-Off Reversal",
        "direction": "reversal",
    },
    {
        "id": "stretched_reversal",
        "name": "Stretched Mean Reversion",
        "direction": "reversal",
    },
    {
        "id": "volume_rejection_reversal",
        "name": "Volume Rejection",
        "direction": "reversal",
    },
    {
        "id": "squeeze_release_continue",
        "name": "Squeeze Release",
        "direction": "continuation",
    },
]


def _playbook_by_id(playbook_id: str) -> dict | None:
    return next((playbook for playbook in _PLAYBOOKS if playbook["id"] == playbook_id), None)


def _playbook_condition(row: pd.Series, playbook_id: str, horizon: int) -> bool:
    side = row.get("touched_side")
    if side not in ("Upper", "Lower"):
        return False

    side_ret_3d = _safe_num(row.get("side_ret_3d"), 0.0)
    side_ret_5d = _safe_num(row.get("side_ret_5d"), 0.0)
    side_ret_10d = _safe_num(row.get("side_ret_10d"), 0.0)
    side_ret_20d = _safe_num(row.get("side_ret_20d"), 0.0)
    side_dist_ma20 = _safe_num(row.get("side_dist_ma20_atr"), 0.0)
    side_ma20_slope = _safe_num(row.get("side_ma20_slope_5"), 0.0)
    side_rsi = _safe_num(row.get("side_rsi_deviation"), 0.0)
    side_mfi = _safe_num(row.get("side_mfi_deviation"), 0.0)
    side_qqq = _safe_num(row.get("side_qqq_ret_5d"), 0.0)
    side_xlk = _safe_num(row.get("side_xlk_ret_5d"), 0.0)
    pressure = _safe_num(row.get("side_weighted_volume_pressure_5"), 0.0)
    touch_depth = _safe_num(row.get("touch_depth_atr"), 0.0)
    reentry = _safe_num(row.get("touch_reentry_signal"), 0.0)
    wick_minus_body = _safe_num(row.get("touch_wick_minus_body"), 0.0)
    close_location = _safe_num(row.get("side_close_location"), 0.0)
    consecutive = _safe_num(row.get("consecutive_touch_count"), 0.0)
    band_rank = _safe_num(row.get("band_width_percentile"), 0.5)
    bandwidth_change = _safe_num(row.get("bandwidth_change_5d"), 0.0)
    rel_volume = _safe_num(row.get("rel_volume_20"), 1.0)
    adx = _safe_num(row.get("ADX14"), 0.0)

    if playbook_id == "lower_falling_knife_continue":
        return (
            side == "Lower"
            and side_ret_5d >= 0.035
            and side_dist_ma20 >= 0.75
            and wick_minus_body < 0.25
            and (reentry <= 0.0 or consecutive >= 2)
            and pressure < 0.15
            and touch_depth < 0.55
        )

    if playbook_id == "upper_breakout_continue":
        return (
            side == "Upper"
            and side_ret_5d >= 0.035
            and (band_rank >= 0.65 or bandwidth_change >= 0.025 or adx >= 25)
            and wick_minus_body < 0.25
            and pressure > -0.15
            and (consecutive <= 3 or band_rank < 0.75)
            and side_ret_20d < 0.16
        )

    if playbook_id == "band_walk_continue":
        return (
            consecutive >= 3
            and side_ret_3d >= 0.018
            and reentry <= 0.0
            and wick_minus_body < 0.20
            and pressure < 0.35
        )

    if playbook_id == "context_trend_continue":
        return (
            side_ret_5d >= 0.025
            and side_ma20_slope >= -0.002
            and side_qqq >= 0.002
            and side_xlk >= 0.002
            and wick_minus_body < 0.25
        )

    if playbook_id == "reentry_reversal":
        return (
            reentry > 0.0
            and wick_minus_body >= 0.10
            and close_location <= 0.25
            and touch_depth >= 0.05
        )

    if playbook_id == "capitulation_reversal":
        return (
            side == "Lower"
            and consecutive >= 4
            and side_ret_5d >= 0.045
            and (pressure >= 0.20 or rel_volume >= 1.25)
            and side_qqq <= 0.015
        )

    if playbook_id == "blowoff_reversal":
        return (
            side == "Upper"
            and (side_ret_10d >= 0.07 or side_ret_20d >= 0.12)
            and side_rsi >= 0.70
            and (wick_minus_body >= 0.15 or pressure < -0.05)
            and pressure < 0.35
            and not (band_rank > 0.80 and bandwidth_change > 0.08)
        )

    if playbook_id == "stretched_reversal":
        return (
            side_dist_ma20 >= 1.75
            and touch_depth >= 0.25
            and wick_minus_body >= 0.15
            and (reentry > 0.0 or close_location <= 0.0)
            and pressure < 0.35
            and not (side == "Upper" and band_rank > 0.80 and bandwidth_change > 0.08)
            and side_mfi >= 0.25
        )

    if playbook_id == "volume_rejection_reversal":
        return wick_minus_body >= 0.28 and rel_volume >= 1.10 and close_location <= 0.35

    if playbook_id == "squeeze_release_continue":
        return (
            band_rank <= 0.35
            and side_ret_3d >= 0.018
            and touch_depth >= 0.15
            and wick_minus_body < 0.20
            and horizon <= 10
        )

    return False


def _actual_direction_for_index(feature_df: pd.DataFrame, idx: int, horizon: int) -> str | None:
    if idx + horizon >= len(feature_df):
        return None
    row = feature_df.iloc[idx]
    side = row.get("touched_side")
    if side not in ("Upper", "Lower"):
        return None
    signal_close = _safe_num(row.get("close"), np.nan)
    outcome_close = _safe_num(feature_df.iloc[idx + horizon].get("close"), np.nan)
    if not np.isfinite(signal_close) or not np.isfinite(outcome_close):
        return None
    return _actual_direction(side, signal_close, outcome_close, _flat_tolerance_for_row(row))


def _playbook_training_labels(feature_df: pd.DataFrame, target_idx: int, horizon: int, playbook_id: str) -> list[str]:
    labels: list[str] = []
    max_idx = min(target_idx - horizon, len(feature_df) - horizon - 1)
    if max_idx < 0:
        return labels
    for idx in range(max_idx + 1):
        row = feature_df.iloc[idx]
        if _safe_bool(row.get("event_risk_blocked")):
            continue
        if not _playbook_condition(row, playbook_id, horizon):
            continue
        actual = _actual_direction_for_index(feature_df, idx, horizon)
        if actual in ("continuation", "reversal"):
            labels.append(actual)
    return labels


def _score_playbook(feature_df: pd.DataFrame, target_idx: int, horizon: int, playbook: dict) -> dict | None:
    labels = _playbook_training_labels(feature_df, target_idx, horizon, playbook["id"])
    count = len(labels)
    if count < _PLAYBOOK_MIN_MATCHES:
        return None
    direction = playbook["direction"]
    correct = sum(1 for label in labels if label == direction)
    precision = correct / count
    posterior = (correct + 1.0) / (count + 2.0)
    wilson = _wilson_lower_bound(correct, count)
    recent_labels = labels[-min(8, len(labels)) :]
    recent_correct = sum(1 for label in recent_labels if label == direction)
    recent_precision = recent_correct / len(recent_labels) if recent_labels else precision
    score = (0.50 * posterior) + (0.25 * wilson) + (0.25 * recent_precision)
    return {
        "id": playbook["id"],
        "name": playbook["name"],
        "direction": direction,
        "match_count": count,
        "correct_count": correct,
        "precision": round(precision, 6),
        "posterior_probability": round(posterior, 6),
        "wilson_lower_bound": round(wilson, 6),
        "recent_precision": round(recent_precision, 6),
        "score": round(score, 6),
    }


def _playbook_training_summary(feature_df: pd.DataFrame, target_idx: int, horizon: int) -> tuple[int, int, int]:
    labels: list[int] = []
    max_idx = min(target_idx - horizon, len(feature_df) - horizon - 1)
    if max_idx < 0:
        return 0, 0, 0
    for idx in range(max_idx + 1):
        actual = _actual_direction_for_index(feature_df, idx, horizon)
        if actual == "continuation":
            labels.append(1)
        elif actual == "reversal":
            labels.append(0)
    return len(labels), labels.count(1), labels.count(0)


def _evaluate_horizon_with_playbooks(feature_df: pd.DataFrame, row_index: int, horizon: int) -> dict:
    row = feature_df.iloc[row_index]
    training_count, continuation_count, reversal_count = _playbook_training_summary(feature_df, row_index, horizon)
    triggered = [playbook for playbook in _PLAYBOOKS if _playbook_condition(row, playbook["id"], horizon)]
    if not triggered:
        result = _no_prediction_horizon(
            horizon,
            "no_playbook_match",
            training_sample_count=training_count,
            positive_count=continuation_count,
            negative_count=reversal_count,
        )
        result["playbook_candidates"] = []
        result["model"]["type"] = "walk_forward_playbook_miner"
        return result

    scored = [
        score
        for playbook in triggered
        for score in [_score_playbook(feature_df, row_index, horizon, playbook)]
        if score is not None
    ]
    if not scored:
        result = _no_prediction_horizon(
            horizon,
            "insufficient_playbook_history",
            training_sample_count=training_count,
            positive_count=continuation_count,
            negative_count=reversal_count,
        )
        result["playbook_candidates"] = [
            {"id": playbook["id"], "name": playbook["name"], "direction": playbook["direction"]}
            for playbook in triggered
        ]
        result["model"]["type"] = "walk_forward_playbook_miner"
        return result

    scored = sorted(scored, key=lambda item: (item["score"], item["match_count"]), reverse=True)
    best = scored[0]
    enough_edge = (
        best["precision"] >= _PLAYBOOK_MIN_PRECISION
        and best["posterior_probability"] >= _PLAYBOOK_MIN_POSTERIOR
        and best["wilson_lower_bound"] >= _PLAYBOOK_MIN_WILSON
    )
    direction = best["direction"]
    continuation_probability = (
        best["posterior_probability"] if direction == "continuation" else 1.0 - best["posterior_probability"]
    )
    reversal_probability = 1.0 - continuation_probability
    confidence_score = int(round(max(continuation_probability, reversal_probability) * 100))

    if not enough_edge:
        status = "no_prediction"
        predicted_direction = None
        reason = "weak_playbook_edge"
    else:
        status = "prediction"
        predicted_direction = direction
        reason = None

    model = _model_metadata(
        training_count,
        continuation_count,
        reversal_count,
        validation_accuracy=best["precision"],
        validation_precision=best["precision"],
        candidate_count=len(scored),
    )
    model["type"] = "walk_forward_playbook_miner"
    return {
        "status": status,
        "predicted_direction": predicted_direction,
        "continuation_probability": round(continuation_probability, 6),
        "reversal_probability": round(reversal_probability, 6),
        "continuation_confidence_score": int(round(continuation_probability * 100)),
        "reversal_confidence_score": int(round(reversal_probability * 100)),
        "continuation_validation_precision": best["precision"] if direction == "continuation" else None,
        "reversal_validation_precision": best["precision"] if direction == "reversal" else None,
        "continuation_validation_count": best["match_count"] if direction == "continuation" else 0,
        "reversal_validation_count": best["match_count"] if direction == "reversal" else 0,
        "validation_policy": None,
        "confidence_score": confidence_score,
        "threshold": _PREDICTION_THRESHOLD,
        "no_prediction_reason": reason,
        "reversal_veto_reason": None,
        "analog_evidence": {
            "status": "ready",
            "direction": direction,
            "posterior_probability": best["posterior_probability"],
            "neighbor_count": best["match_count"],
            "precision": best["precision"],
            "lower_bound": best["wilson_lower_bound"],
        },
        "analog_override": False,
        "deployment_quality_gate": None,
        "blocked_prediction": None,
        "playbook": best,
        "playbook_candidates": scored[:5],
        "contributions": [
            {
                "horizon": f"{horizon}d",
                "feature": best["name"],
                "value": best["match_count"],
                "impact": direction,
                "contribution": best["score"],
            }
        ],
        "model": model,
    }




__all__ = [name for name in globals() if not name.startswith("__")]
