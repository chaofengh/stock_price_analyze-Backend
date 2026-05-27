from __future__ import annotations

from copy import deepcopy
from typing import Any

import numpy as np
import pandas as pd

from .settings import *
from .features import _safe_num, _to_date_string
from .model import _wilson_lower_bound
from .backtest import _confidence_bucket_metrics, _return_metrics_for_predictions


def _gate_wilson(correct_count: int, sample_count: int) -> float | None:
    if sample_count <= 0:
        return None
    return round(_wilson_lower_bound(correct_count, sample_count, z_value=_DEPLOYMENT_WILSON_Z), 6)


def _signal_min_wilson(direction: str) -> float:
    return (
        _DEPLOY_MIN_SIGNAL_BACKTEST_CONTINUE_WILSON
        if direction == "continuation"
        else _DEPLOY_MIN_SIGNAL_BACKTEST_REVERSE_WILSON
    )


def _signal_gate_key(direction: str | None, signal_id: str | None) -> str:
    return f"{direction or 'unknown'}:{signal_id or 'unknown'}"


def _prediction_signal_id_from_backtest_row(row: dict) -> str:
    return str(row.get("signal_model_id") or row.get("signal_tier") or "unknown")


def _prediction_group_key(prediction: dict) -> str:
    return _signal_gate_key(
        prediction.get("predicted_direction"),
        _prediction_signal_id_from_backtest_row(prediction),
    )


def _internal_empirical_quality_gate(prediction: dict) -> dict | None:
    signal_id = _clean_key_part(_prediction_signal_id_from_backtest_row(prediction))
    if not signal_id or not signal_id.startswith("empirical_"):
        return None

    direction = prediction.get("predicted_direction")
    if direction not in ("continuation", "reversal"):
        return None

    min_accuracy = (
        _EMPIRICAL_CONTINUATION_MIN_PRECISION
        if direction == "continuation"
        else _EMPIRICAL_REVERSAL_MIN_PRECISION
    )
    min_wilson = (
        _EMPIRICAL_CONTINUATION_MIN_WILSON
        if direction == "continuation"
        else _EMPIRICAL_REVERSAL_MIN_WILSON
    )
    min_recent = (
        _EMPIRICAL_CONTINUATION_MIN_RECENT_PRECISION
        if direction == "continuation"
        else _EMPIRICAL_REVERSAL_MIN_RECENT_PRECISION
    )
    min_prediction_count = (
        _EMPIRICAL_CONTINUATION_MIN_MATCHES
        if direction == "continuation"
        else _EMPIRICAL_REVERSAL_MIN_MATCHES
    )
    prediction_count = int(_safe_num(prediction.get("signal_match_count"), 0))
    correct_count = int(_safe_num(prediction.get("signal_correct_count"), 0))
    accuracy = _safe_num(prediction.get("signal_precision"), np.nan)
    wilson = _safe_num(prediction.get("signal_wilson_lower_bound"), np.nan)
    recent_precision = _safe_num(prediction.get("signal_recent_precision"), np.nan)
    failures: list[str] = []

    if prediction_count < min_prediction_count:
        failures.append("insufficient_internal_empirical_sample")
    if not np.isfinite(accuracy) or accuracy < min_accuracy:
        failures.append(f"weak_{direction}_internal_empirical_accuracy")
    if not np.isfinite(wilson) or wilson < min_wilson:
        failures.append(f"weak_{direction}_internal_empirical_wilson")
    if not np.isfinite(recent_precision) or recent_precision < min_recent:
        failures.append(f"weak_{direction}_internal_empirical_recent_precision")

    signal_date = _prediction_date(prediction.get("signal_date"))
    return {
        "status": "quarantined" if failures else "passed",
        "deployment_enabled": not failures,
        "direction": direction,
        "signal_id": signal_id,
        "evidence_scope": "internal_empirical_regime",
        "evidence_key": signal_id,
        "failures": failures,
        "min_prediction_count": min_prediction_count,
        "min_accuracy": min_accuracy,
        "min_wilson_lower_bound": min_wilson,
        "min_recent_precision": min_recent,
        "wilson_z_value": _DEPLOYMENT_WILSON_Z,
        "raw_prediction_count": prediction_count,
        "raw_correct_count": correct_count,
        "raw_accuracy": round(accuracy, 6) if np.isfinite(accuracy) else None,
        "raw_wilson_lower_bound": round(wilson, 6) if np.isfinite(wilson) else None,
        "raw_recent_precision": round(recent_precision, 6) if np.isfinite(recent_precision) else None,
        "gate_date": _to_date_string(signal_date),
    }


def _prediction_book_metrics(predictions: list[dict]) -> dict:
    prediction_count = len(predictions)
    correct_count = sum(1 for item in predictions if item.get("is_correct"))
    continuation = [item for item in predictions if item.get("predicted_direction") == "continuation"]
    reversal = [item for item in predictions if item.get("predicted_direction") == "reversal"]
    continuation_correct = sum(1 for item in continuation if item.get("is_correct"))
    reversal_correct = sum(1 for item in reversal if item.get("is_correct"))
    return {
        "prediction_count": prediction_count,
        "correct_count": correct_count,
        "accuracy": correct_count / prediction_count if prediction_count else None,
        "continuation_call_count": len(continuation),
        "continuation_accuracy": continuation_correct / len(continuation) if continuation else None,
        "reversal_call_count": len(reversal),
        "reversal_accuracy": reversal_correct / len(reversal) if reversal else None,
        **_return_metrics_for_predictions(predictions),
    }


def _signal_quality_gate_from_rows(
    direction: str,
    signal_id: str | None,
    predictions: list[dict],
    *,
    direction_gate: dict | None = None,
    gate_date: str | None = None,
    evidence_scope: str = "exact_signal",
    evidence_key: str | None = None,
    min_prediction_count: int | None = None,
    min_accuracy: float | None = None,
    min_wilson: float | None = None,
) -> dict:
    min_accuracy = min_accuracy if min_accuracy is not None else (
        _DEPLOY_MIN_BACKTEST_CONTINUE_ACCURACY
        if direction == "continuation"
        else _DEPLOY_MIN_BACKTEST_REVERSE_ACCURACY
    )
    min_wilson = min_wilson if min_wilson is not None else _signal_min_wilson(direction)
    min_prediction_count = (
        int(min_prediction_count)
        if min_prediction_count is not None
        else _DEPLOY_MIN_SIGNAL_BACKTEST_CALLS
    )
    prediction_count = len(predictions)
    correct_count = sum(1 for item in predictions if item.get("is_correct"))
    accuracy = correct_count / prediction_count if prediction_count else None
    wilson = _gate_wilson(correct_count, prediction_count)
    metrics = _prediction_book_metrics(predictions)
    expected_return = metrics.get("expected_return")
    expected_atr_return = metrics.get("expected_atr_return")
    failures: list[str] = []

    if prediction_count < min_prediction_count:
        failures.append("insufficient_signal_sample")
    if accuracy is None or _safe_num(accuracy, 0.0) < min_accuracy:
        failures.append(f"weak_{direction}_signal_accuracy")
    if wilson is None or wilson < min_wilson:
        failures.append(f"weak_{direction}_signal_wilson")
    if expected_return is None or _safe_num(expected_return, 0.0) <= _DEPLOY_MIN_SIGNAL_EXPECTED_RETURN:
        failures.append("nonpositive_signal_expected_return")

    return {
        "status": "quarantined" if failures else "passed",
        "deployment_enabled": not failures,
        "direction": direction,
        "signal_id": signal_id or "unknown",
        "evidence_scope": evidence_scope,
        "evidence_key": evidence_key or signal_id or "unknown",
        "failures": failures,
        "min_prediction_count": min_prediction_count,
        "min_accuracy": min_accuracy,
        "min_wilson_lower_bound": min_wilson,
        "wilson_z_value": _DEPLOYMENT_WILSON_Z,
        "min_expected_return": _DEPLOY_MIN_SIGNAL_EXPECTED_RETURN,
        "raw_prediction_count": prediction_count,
        "raw_correct_count": correct_count,
        "raw_accuracy": round(accuracy, 6) if accuracy is not None else None,
        "raw_wilson_lower_bound": wilson,
        "raw_expected_return": expected_return,
        "raw_expected_atr_return": expected_atr_return,
        "direction_gate_status": (direction_gate or {}).get("status"),
        "gate_date": gate_date,
    }


def _prediction_date(value: Any) -> pd.Timestamp | None:
    if value is None:
        return None
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(timestamp):
        return None
    return timestamp.normalize()


def _prediction_sort_key(prediction: dict) -> tuple:
    signal_date = _prediction_date(prediction.get("signal_date"))
    outcome_date = _prediction_date(prediction.get("outcome_date"))
    return (
        signal_date or pd.Timestamp.max,
        outcome_date or pd.Timestamp.max,
        str(prediction.get("signal_model_id") or ""),
    )


def _prediction_identity_key(prediction: dict) -> tuple:
    return (
        prediction.get("signal_date"),
        prediction.get("outcome_date"),
        prediction.get("horizon_days"),
        prediction.get("touched_side"),
        prediction.get("predicted_direction"),
        prediction.get("actual_direction"),
        prediction.get("signal_model_id"),
        prediction.get("signal_tier"),
    )


def _unique_prediction_rows(predictions: list[dict]) -> list[dict]:
    unique: list[dict] = []
    seen: set[tuple] = set()
    for prediction in predictions:
        if not isinstance(prediction, dict):
            continue
        key = _prediction_identity_key(prediction)
        if key in seen:
            continue
        seen.add(key)
        unique.append(prediction)
    return unique


def _confidence_bucket(confidence_score: Any) -> str:
    score = _safe_num(confidence_score, -1)
    if score < 0:
        return "unknown"
    score_int = int(max(0, min(100, round(score))))
    if score_int >= 95:
        return "95_100"
    if score_int >= 90:
        return "90_94"
    if score_int >= 85:
        return "85_89"
    if score_int >= 80:
        return "80_84"
    return "lt_80"


def _clean_key_part(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text or text.lower() == "none":
        return None
    return text


def _prediction_evidence_candidates(prediction: dict) -> list[dict]:
    direction = _clean_key_part(prediction.get("predicted_direction"))
    if direction not in ("continuation", "reversal"):
        return []
    side = _clean_key_part(prediction.get("touched_side")) or "unknown_side"
    signal_id = _clean_key_part(_prediction_signal_id_from_backtest_row(prediction))
    signal_tier = _clean_key_part(prediction.get("signal_tier"))
    signal_model = _clean_key_part(prediction.get("signal_model"))
    confidence = _confidence_bucket(prediction.get("confidence_score"))

    candidates: list[dict] = []
    if signal_id:
        candidates.append(
            {
                "scope": "exact_signal_side",
                "key": f"{direction}|{side}|{signal_id}",
                "signal_id": signal_id,
                "min_prediction_count": _DEPLOY_MIN_SIGNAL_BACKTEST_CALLS,
                "min_accuracy": _DEPLOY_MIN_SIGNAL_BACKTEST_ACCURACY,
                "min_wilson": _signal_min_wilson(direction),
                "block_broader_on_failure": True,
            }
        )
        candidates.append(
            {
                "scope": "exact_signal",
                "key": f"{direction}|{signal_id}",
                "signal_id": signal_id,
                "min_prediction_count": _DEPLOY_MIN_SIGNAL_BACKTEST_CALLS,
                "min_accuracy": _DEPLOY_MIN_SIGNAL_BACKTEST_ACCURACY,
                "min_wilson": _signal_min_wilson(direction),
                "block_broader_on_failure": True,
            }
        )
    fallback_min_wilson = (
        _DEPLOY_MIN_FALLBACK_SIGNAL_CONTINUE_WILSON
        if direction == "continuation"
        else _DEPLOY_MIN_FALLBACK_SIGNAL_REVERSE_WILSON
    )
    fallback = {
        "min_prediction_count": _DEPLOY_MIN_FALLBACK_SIGNAL_BACKTEST_CALLS,
        "min_accuracy": _DEPLOY_MIN_FALLBACK_SIGNAL_ACCURACY,
        "min_wilson": fallback_min_wilson,
    }
    if signal_model:
        candidates.append(
            {
                "scope": "model_side_confidence",
                "key": f"{direction}|{side}|{signal_model}|{confidence}",
                "signal_id": signal_model,
                **fallback,
            }
        )
        candidates.append(
            {
                "scope": "model_confidence",
                "key": f"{direction}|{signal_model}|{confidence}",
                "signal_id": signal_model,
                **fallback,
            }
        )
    if signal_tier:
        candidates.append(
            {
                "scope": "tier_side_confidence",
                "key": f"{direction}|{side}|{signal_tier}|{confidence}",
                "signal_id": signal_tier,
                **fallback,
            }
        )
        candidates.append(
            {
                "scope": "tier_confidence",
                "key": f"{direction}|{signal_tier}|{confidence}",
                "signal_id": signal_tier,
                **fallback,
            }
        )
        broad_family = {
            "min_prediction_count": _DEPLOY_MIN_BROAD_FAMILY_BACKTEST_CALLS,
            "min_accuracy": _DEPLOY_MIN_BROAD_FAMILY_ACCURACY,
            "min_wilson": _DEPLOY_MIN_BROAD_FAMILY_WILSON,
        }
        candidates.append(
            {
                "scope": "tier_side_family",
                "key": f"{direction}|{side}|{signal_tier}",
                "signal_id": signal_tier,
                **broad_family,
            }
        )
        candidates.append(
            {
                "scope": "tier_family",
                "key": f"{direction}|{signal_tier}",
                "signal_id": signal_tier,
                **broad_family,
            }
        )
    broad_direction = {
        "min_prediction_count": _DEPLOY_MIN_BROAD_FAMILY_BACKTEST_CALLS,
        "min_accuracy": _DEPLOY_MIN_BROAD_FAMILY_ACCURACY,
        "min_wilson": _DEPLOY_MIN_BROAD_FAMILY_WILSON,
    }
    candidates.append(
        {
            "scope": "direction_side_confidence",
            "key": f"{direction}|{side}|{confidence}",
            "signal_id": f"{direction}_{side}_{confidence}",
            **broad_direction,
        }
    )
    candidates.append(
        {
            "scope": "direction_side",
            "key": f"{direction}|{side}",
            "signal_id": f"{direction}_{side}",
            **broad_direction,
        }
    )
    return candidates


def _candidate_key_for_prediction(prediction: dict, scope: str) -> str | None:
    for candidate in _prediction_evidence_candidates(prediction):
        if candidate["scope"] == scope:
            return candidate["key"]
    return None


def _prior_completed_predictions(
    predictions: list[dict],
    *,
    scope: str,
    key: str,
    signal_date: pd.Timestamp | None,
) -> list[dict]:
    if signal_date is None:
        return []
    out: list[dict] = []
    for item in predictions:
        if _candidate_key_for_prediction(item, scope) != key:
            continue
        outcome_date = _prediction_date(item.get("outcome_date"))
        if outcome_date is not None and outcome_date < signal_date:
            out.append(item)
    return out


def _deployment_gate_for_prediction(prediction: dict, prior_rows: list[dict]) -> dict:
    direction = prediction.get("predicted_direction")
    signal_date = _prediction_date(prediction.get("signal_date"))
    internal_gate = _internal_empirical_quality_gate(prediction)
    if internal_gate is not None and internal_gate.get("deployment_enabled", False):
        return internal_gate
    best_gate: dict | None = internal_gate
    for candidate in _prediction_evidence_candidates(prediction):
        prior = _prior_completed_predictions(
            prior_rows,
            scope=candidate["scope"],
            key=candidate["key"],
            signal_date=signal_date,
        )
        gate = _signal_quality_gate_from_rows(
            direction,
            candidate["signal_id"],
            prior,
            direction_gate={},
            gate_date=_to_date_string(signal_date),
            evidence_scope=candidate["scope"],
            evidence_key=candidate["key"],
            min_prediction_count=candidate["min_prediction_count"],
            min_accuracy=candidate.get("min_accuracy"),
            min_wilson=candidate["min_wilson"],
        )
        if gate.get("deployment_enabled", False):
            return gate
        best_gate = gate
        mature_failed_exact = candidate.get("block_broader_on_failure") and int(
            _safe_num(gate.get("raw_prediction_count"), 0)
        ) >= int(gate.get("min_prediction_count") or 0)
        if mature_failed_exact:
            return gate
    return best_gate or _signal_quality_gate_from_rows(
        str(direction or "unknown"),
        _prediction_signal_id_from_backtest_row(prediction),
        [],
        direction_gate={},
        gate_date=_to_date_string(signal_date),
    )


def _horizon_decision_evidence_row(
    decision: dict,
    horizon_key: str,
    horizon_decision: dict,
    signal_date: Any,
) -> dict:
    playbook = horizon_decision.get("playbook") or {}
    profile = playbook.get("profile") or {}
    return {
        "signal_date": _to_date_string(signal_date),
        "outcome_date": None,
        "horizon_days": int(str(horizon_key).lower().replace("d", "") or 0),
        "touched_side": decision.get("touched_side"),
        "predicted_direction": horizon_decision.get("predicted_direction"),
        "actual_direction": None,
        "is_correct": False,
        "confidence_score": horizon_decision.get("confidence_score"),
        "signal_model": playbook.get("name"),
        "signal_model_id": playbook.get("id") or profile.get("id"),
        "signal_precision": playbook.get("precision"),
        "signal_match_count": playbook.get("match_count"),
        "signal_correct_count": playbook.get("correct_count"),
        "signal_wilson_lower_bound": playbook.get("wilson_lower_bound"),
        "signal_recent_precision": playbook.get("recent_precision"),
        "signal_tier": playbook.get("tier") or profile.get("tier"),
    }


def _deployment_gate_for_horizon_decision(
    decision: dict,
    horizon_key: str,
    horizon_decision: dict,
    prior_predictions: list[dict],
    signal_date: Any,
) -> dict:
    evidence_row = _horizon_decision_evidence_row(
        decision,
        horizon_key,
        horizon_decision,
        signal_date,
    )
    return _deployment_gate_for_prediction(evidence_row, prior_predictions)


def _walk_forward_deployed_predictions(
    raw_result: dict,
    evidence_predictions: list[dict] | None = None,
) -> tuple[list[dict], int]:
    raw_predictions = sorted(
        [item for item in raw_result.get("predictions", []) if isinstance(item, dict)],
        key=_prediction_sort_key,
    )
    prior_rows = sorted(
        _unique_prediction_rows(
            evidence_predictions
            if evidence_predictions is not None
            else raw_result.get("predictions", [])
        ),
        key=_prediction_sort_key,
    )
    deployed: list[dict] = []
    blocked_count = 0

    for prediction in raw_predictions:
        direction = prediction.get("predicted_direction")
        if direction not in ("continuation", "reversal"):
            blocked_count += 1
            continue
        gate = _deployment_gate_for_prediction(prediction, prior_rows)
        if not gate.get("deployment_enabled", False):
            blocked_count += 1
            continue
        deployed_row = deepcopy(prediction)
        deployed_row["deployment_quality_gate"] = deepcopy(gate)
        deployed.append(deployed_row)

    return deployed, blocked_count


def _signal_tier_counts(predictions: list[dict]) -> dict:
    counts: dict[str, int] = {}
    for prediction in predictions:
        tier = prediction.get("signal_tier")
        if tier:
            counts[str(tier)] = counts.get(str(tier), 0) + 1
    return counts


def _deployment_backtest_from_raw(raw_result: dict, evidence_result: dict | None = None) -> dict:
    evidence_predictions = None
    if isinstance(evidence_result, dict):
        evidence_predictions = evidence_result.get("predictions", [])
    deployed_predictions, blocked_count = _walk_forward_deployed_predictions(
        raw_result,
        evidence_predictions=evidence_predictions,
    )
    eligible_count = int(_safe_num(raw_result.get("eligible_touch_count"), 0))
    prediction_count = len(deployed_predictions)
    metrics = _prediction_book_metrics(deployed_predictions)
    deployed_keys = {_prediction_identity_key(prediction) for prediction in deployed_predictions}
    removed_predictions = [
        prediction
        for prediction in raw_result.get("predictions", [])
        if isinstance(prediction, dict)
        and _prediction_identity_key(prediction) not in deployed_keys
    ]
    missed_reversal_count = int(_safe_num(raw_result.get("missed_reversal_count"), 0)) + sum(
        1
        for prediction in removed_predictions
        if prediction.get("actual_direction") == "reversal"
        and prediction.get("predicted_direction") == "reversal"
    )

    result = deepcopy(raw_result)
    result.update(
        {
            "prediction_count": prediction_count,
            "sample_count": prediction_count,
            "no_prediction_count": max(0, eligible_count - prediction_count),
            "coverage": round(prediction_count / eligible_count, 6) if eligible_count else None,
            "correct_count": metrics["correct_count"],
            "accuracy": round(metrics["accuracy"], 6) if metrics.get("accuracy") is not None else None,
            "continuation_call_count": metrics["continuation_call_count"],
            "continuation_correct_count": sum(
                1
                for item in deployed_predictions
                if item.get("predicted_direction") == "continuation" and item.get("is_correct")
            ),
            "continuation_accuracy": (
                round(metrics["continuation_accuracy"], 6)
                if metrics.get("continuation_accuracy") is not None
                else None
            ),
            "reversal_call_count": metrics["reversal_call_count"],
            "reversal_correct_count": sum(
                1
                for item in deployed_predictions
                if item.get("predicted_direction") == "reversal" and item.get("is_correct")
            ),
            "reversal_accuracy": (
                round(metrics["reversal_accuracy"], 6)
                if metrics.get("reversal_accuracy") is not None
                else None
            ),
            "missed_reversal_count": missed_reversal_count,
            "confidence_buckets": _confidence_bucket_metrics(deployed_predictions),
            "signal_tier_counts": _signal_tier_counts(deployed_predictions),
            "predictions": deployed_predictions,
            "recent_predictions": list(reversed(deployed_predictions[-20:])),
            "deployment_filtered_prediction_count": blocked_count,
        }
    )
    result.update(_return_metrics_for_predictions(deployed_predictions))
    return result


__all__ = [name for name in globals() if not name.startswith("__")]
