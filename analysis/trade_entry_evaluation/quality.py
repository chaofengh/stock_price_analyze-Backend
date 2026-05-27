from __future__ import annotations

from typing import Any

from .settings import *
from .features import *
from .model import *
from .backtest import *
from .deployment import *
from .adaptive import _adaptive_training_summary
from .empirical import _empirical_regime_attrs
from .playbooks import _actual_direction_for_index

def _deployment_quality_gate(backtest: dict) -> dict:
    prediction_count = int(_safe_num(backtest.get("prediction_count"), 0))
    raw_prediction_count = int(
        _safe_num(
            backtest.get("raw_prediction_count"),
            _safe_num(backtest.get("deployment_filtered_prediction_count"), 0),
        )
    )
    failures: list[str] = []

    if prediction_count == 0:
        if raw_prediction_count > 0:
            failures.append("all_raw_predictions_quarantined")
        return {
            "status": "quarantined" if failures else "idle",
            "deployment_enabled": not failures,
            "failures": failures,
            "min_prediction_count": _DEPLOY_MIN_BACKTEST_CALLS,
            "min_accuracy": _DEPLOY_MIN_BACKTEST_ACCURACY,
            "min_reverse_accuracy": _DEPLOY_MIN_BACKTEST_REVERSE_ACCURACY,
            "min_continue_accuracy": _DEPLOY_MIN_BACKTEST_CONTINUE_ACCURACY,
            "min_wilson_lower_bound": _DEPLOY_MIN_BACKTEST_WILSON,
            "min_reverse_wilson_lower_bound": _DEPLOY_MIN_BACKTEST_REVERSE_WILSON,
            "min_continue_wilson_lower_bound": _DEPLOY_MIN_BACKTEST_CONTINUE_WILSON,
            "wilson_z_value": _DEPLOYMENT_WILSON_Z,
            "raw_prediction_count": raw_prediction_count,
            "raw_correct_count": 0,
            "raw_accuracy": backtest.get("raw_accuracy"),
            "raw_wilson_lower_bound": None,
            "raw_reverse_accuracy": backtest.get("raw_reverse_accuracy"),
            "raw_reverse_wilson_lower_bound": None,
            "raw_continue_accuracy": backtest.get("raw_continue_accuracy"),
            "raw_continue_wilson_lower_bound": None,
        }

    accuracy = backtest.get("accuracy")
    correct_count = _gate_correct_count(backtest.get("correct_count"), accuracy, prediction_count)
    wilson = _gate_wilson(correct_count, prediction_count)
    reversal_accuracy = backtest.get("reversal_accuracy")
    continuation_accuracy = backtest.get("continuation_accuracy")
    reversal_call_count = int(_safe_num(backtest.get("reversal_call_count"), 0))
    continuation_call_count = int(_safe_num(backtest.get("continuation_call_count"), 0))
    reversal_correct_count = _gate_correct_count(
        backtest.get("reversal_correct_count"),
        reversal_accuracy,
        reversal_call_count,
    )
    continuation_correct_count = _gate_correct_count(
        backtest.get("continuation_correct_count"),
        continuation_accuracy,
        continuation_call_count,
    )
    reversal_wilson = _gate_wilson(reversal_correct_count, reversal_call_count)
    continuation_wilson = _gate_wilson(continuation_correct_count, continuation_call_count)

    if prediction_count < _DEPLOY_MIN_BACKTEST_CALLS:
        failures.append("insufficient_deployed_sample")
    if accuracy is None or _safe_num(accuracy, 0.0) < _DEPLOY_MIN_BACKTEST_ACCURACY:
        failures.append("weak_deployed_accuracy")
    if wilson is None or wilson < _DEPLOY_MIN_BACKTEST_WILSON:
        failures.append("weak_deployed_wilson")
    if reversal_call_count > 0 and (
        reversal_accuracy is None or _safe_num(reversal_accuracy, 0.0) < _DEPLOY_MIN_BACKTEST_REVERSE_ACCURACY
    ):
        failures.append("weak_reverse_accuracy")
    if reversal_call_count > 0 and (
        reversal_wilson is None or reversal_wilson < _DEPLOY_MIN_BACKTEST_REVERSE_WILSON
    ):
        failures.append("weak_reverse_wilson")
    if continuation_call_count > 0 and (
        continuation_accuracy is None or _safe_num(continuation_accuracy, 0.0) < _DEPLOY_MIN_BACKTEST_CONTINUE_ACCURACY
    ):
        failures.append("weak_continue_accuracy")
    if continuation_call_count > 0 and (
        continuation_wilson is None or continuation_wilson < _DEPLOY_MIN_BACKTEST_CONTINUE_WILSON
    ):
        failures.append("weak_continue_wilson")

    return {
        "status": "quarantined" if failures else "passed",
        "deployment_enabled": not failures,
        "failures": failures,
        "min_prediction_count": _DEPLOY_MIN_BACKTEST_CALLS,
        "min_accuracy": _DEPLOY_MIN_BACKTEST_ACCURACY,
        "min_reverse_accuracy": _DEPLOY_MIN_BACKTEST_REVERSE_ACCURACY,
        "min_continue_accuracy": _DEPLOY_MIN_BACKTEST_CONTINUE_ACCURACY,
        "min_wilson_lower_bound": _DEPLOY_MIN_BACKTEST_WILSON,
        "min_reverse_wilson_lower_bound": _DEPLOY_MIN_BACKTEST_REVERSE_WILSON,
        "min_continue_wilson_lower_bound": _DEPLOY_MIN_BACKTEST_CONTINUE_WILSON,
        "wilson_z_value": _DEPLOYMENT_WILSON_Z,
        "raw_prediction_count": prediction_count,
        "raw_correct_count": correct_count,
        "raw_accuracy": accuracy,
        "raw_wilson_lower_bound": wilson,
        "raw_reverse_accuracy": reversal_accuracy,
        "raw_reverse_wilson_lower_bound": reversal_wilson,
        "raw_continue_accuracy": continuation_accuracy,
        "raw_continue_wilson_lower_bound": continuation_wilson,
    }


def _horizon_from_key(horizon_key: str) -> int:
    try:
        return int(str(horizon_key).lower().replace("d", ""))
    except (TypeError, ValueError):
        return 0


def _gate_correct_count(correct_count: Any, accuracy: Any, sample_count: int) -> int:
    raw_correct = int(_safe_num(correct_count, -1))
    if raw_correct >= 0:
        return raw_correct
    if sample_count <= 0 or accuracy is None:
        return 0
    return int(round(_safe_num(accuracy, 0.0) * sample_count))


def _direction_min_wilson(direction: str) -> float:
    return (
        _DEPLOY_MIN_BACKTEST_CONTINUE_WILSON
        if direction == "continuation"
        else _DEPLOY_MIN_BACKTEST_REVERSE_WILSON
    )


def _quality_gate_blocked_horizon(horizon_key: str, original: dict, gate: dict) -> dict:
    horizon = _horizon_from_key(horizon_key)
    model_meta = original.get("model", {}) if isinstance(original, dict) else {}
    blocked = _no_prediction_horizon(
        horizon,
        "deployment_quality_gate_failed",
        training_sample_count=int(_safe_num(model_meta.get("training_sample_count"), 0)),
        positive_count=int(_safe_num(model_meta.get("continuation_training_count"), 0)),
        negative_count=int(_safe_num(model_meta.get("reversal_training_count"), 0)),
    )
    blocked["deployment_quality_gate"] = deepcopy(gate)
    blocked["blocked_prediction"] = {
        "status": original.get("status"),
        "predicted_direction": original.get("predicted_direction"),
        "continuation_probability": original.get("continuation_probability"),
        "reversal_probability": original.get("reversal_probability"),
        "confidence_score": original.get("confidence_score"),
        "no_prediction_reason": original.get("no_prediction_reason"),
    }
    blocked["model"] = deepcopy(model_meta) if model_meta else blocked["model"]
    return blocked


def _direction_quality_gate(backtest: dict, direction: str) -> dict:
    if direction == "continuation":
        call_count = int(_safe_num(backtest.get("continuation_call_count"), 0))
        correct_count = _gate_correct_count(
            backtest.get("continuation_correct_count"),
            backtest.get("continuation_accuracy"),
            call_count,
        )
        accuracy = backtest.get("continuation_accuracy")
        min_accuracy = _DEPLOY_MIN_BACKTEST_CONTINUE_ACCURACY
    else:
        call_count = int(_safe_num(backtest.get("reversal_call_count"), 0))
        correct_count = _gate_correct_count(
            backtest.get("reversal_correct_count"),
            backtest.get("reversal_accuracy"),
            call_count,
        )
        accuracy = backtest.get("reversal_accuracy")
        min_accuracy = _DEPLOY_MIN_BACKTEST_REVERSE_ACCURACY
    wilson = _gate_wilson(correct_count, call_count)
    min_wilson = _direction_min_wilson(direction)

    failures: list[str] = []
    if call_count < _DEPLOY_MIN_BACKTEST_CALLS:
        failures.append("insufficient_direction_sample")
    if accuracy is None or _safe_num(accuracy, 0.0) < min_accuracy:
        failures.append(f"weak_{direction}_accuracy")
    if wilson is None or wilson < min_wilson:
        failures.append(f"weak_{direction}_wilson")

    return {
        "status": "quarantined" if failures else "passed",
        "deployment_enabled": not failures,
        "direction": direction,
        "failures": failures,
        "min_prediction_count": _DEPLOY_MIN_BACKTEST_CALLS,
        "min_accuracy": min_accuracy,
        "min_wilson_lower_bound": min_wilson,
        "wilson_z_value": _DEPLOYMENT_WILSON_Z,
        "raw_prediction_count": call_count,
        "raw_correct_count": correct_count,
        "raw_accuracy": accuracy,
        "raw_wilson_lower_bound": wilson,
    }


def _direction_quality_gates(backtest_by_horizon: dict) -> dict[str, dict]:
    gates: dict[str, dict] = {}
    for horizon_key, result in backtest_by_horizon.items():
        if not isinstance(result, dict):
            continue
        continuation_gate = _direction_quality_gate(result, "continuation")
        reversal_gate = _direction_quality_gate(result, "reversal")
        signal_gates = _signal_quality_gates_for_horizon(result, continuation_gate, reversal_gate)
        disabled_signals = [
            gate_key
            for gate_key, gate in signal_gates.items()
            if not gate.get("deployment_enabled", False)
        ]
        failures = continuation_gate["failures"] + reversal_gate["failures"]
        gates[horizon_key] = {
            "status": "partial" if disabled_signals else "passed",
            "deployment_enabled": True,
            "failures": failures,
            "continuation": continuation_gate,
            "reversal": reversal_gate,
            "signals": signal_gates,
        }
    return gates


def _prediction_signal_id_from_horizon(horizon_decision: dict) -> str:
    playbook = horizon_decision.get("playbook") or {}
    profile = playbook.get("profile") or {}
    return str(playbook.get("id") or profile.get("id") or playbook.get("tier") or "unknown")


def _signal_quality_gates_for_horizon(
    backtest: dict,
    continuation_gate: dict,
    reversal_gate: dict,
) -> dict[str, dict]:
    grouped: dict[str, dict] = {}
    for prediction in backtest.get("predictions", []):
        direction = prediction.get("predicted_direction")
        if direction not in ("continuation", "reversal"):
            continue
        signal_id = _prediction_signal_id_from_backtest_row(prediction)
        key = _signal_gate_key(direction, signal_id)
        bucket = grouped.setdefault(
            key,
            {
                "direction": direction,
                "signal_id": signal_id,
                "prediction_count": 0,
                "correct_count": 0,
                "predictions": [],
            },
        )
        bucket["prediction_count"] += 1
        bucket["predictions"].append(prediction)
        if prediction.get("is_correct"):
            bucket["correct_count"] += 1

    gates: dict[str, dict] = {}
    for key, bucket in grouped.items():
        direction = bucket["direction"]
        direction_gate = continuation_gate if direction == "continuation" else reversal_gate
        gates[key] = _signal_quality_gate_from_rows(
            direction,
            bucket["signal_id"],
            bucket["predictions"],
            direction_gate=direction_gate,
        )
    return gates


def _prediction_book_passes_quality(metrics: dict) -> bool:
    if int(_safe_num(metrics.get("prediction_count"), 0)) <= 0:
        return False
    if _safe_num(metrics.get("accuracy"), 0.0) < _DEPLOY_MIN_BACKTEST_ACCURACY:
        return False
    if int(_safe_num(metrics.get("reversal_call_count"), 0)) > 0 and (
        metrics.get("reversal_accuracy") is None
        or _safe_num(metrics.get("reversal_accuracy"), 0.0) < _DEPLOY_MIN_BACKTEST_REVERSE_ACCURACY
    ):
        return False
    if int(_safe_num(metrics.get("continuation_call_count"), 0)) > 0 and (
        metrics.get("continuation_accuracy") is None
        or _safe_num(metrics.get("continuation_accuracy"), 0.0) < _DEPLOY_MIN_BACKTEST_CONTINUE_ACCURACY
    ):
        return False
    return True


def _accuracy_budget_preserved(
    current_metrics: dict,
    trial_metrics: dict,
    *,
    accuracy_key: str,
    count_key: str,
    correct_key: str,
    min_accuracy: float,
) -> bool:
    current = current_metrics.get(accuracy_key)
    if current is None:
        return True
    trial = trial_metrics.get(accuracy_key)
    if trial is None:
        return False

    current_accuracy = _safe_num(current, 0.0)
    trial_accuracy = _safe_num(trial, 0.0)
    if trial_accuracy < min_accuracy:
        return False
    if trial_accuracy + _COVERAGE_EXPANSION_MAX_ACCURACY_DROP + _EPS >= current_accuracy:
        return True

    current_count = int(_safe_num(current_metrics.get(count_key), 0))
    trial_count = int(_safe_num(trial_metrics.get(count_key), 0))
    if current_count <= 0 or trial_count <= current_count:
        return False

    current_correct = int(_safe_num(current_metrics.get(correct_key), 0))
    trial_correct = int(_safe_num(trial_metrics.get(correct_key), 0))
    current_lower = _wilson_lower_bound(current_correct, current_count)
    trial_lower = _wilson_lower_bound(trial_correct, trial_count)
    return trial_lower + _EPS >= current_lower


def _prediction_book_preserves_accuracy(current_metrics: dict, trial_metrics: dict) -> bool:
    checks = (
        (
            "accuracy",
            "prediction_count",
            "correct_count",
            _DEPLOY_MIN_BACKTEST_ACCURACY,
        ),
        (
            "continuation_accuracy",
            "continuation_call_count",
            "continuation_correct_count",
            _DEPLOY_MIN_BACKTEST_CONTINUE_ACCURACY,
        ),
        (
            "reversal_accuracy",
            "reversal_call_count",
            "reversal_correct_count",
            _DEPLOY_MIN_BACKTEST_REVERSE_ACCURACY,
        ),
    )
    for accuracy_key, count_key, correct_key, min_accuracy in checks:
        if not _accuracy_budget_preserved(
            current_metrics,
            trial_metrics,
            accuracy_key=accuracy_key,
            count_key=count_key,
            correct_key=correct_key,
            min_accuracy=min_accuracy,
        ):
            return False
    return True


def _prediction_row_key(row: dict) -> tuple:
    return (
        row.get("_repair_row_key")
        or row.get("signal_date")
        or row.get("date"),
        row.get("horizon_days"),
    )


def _candidate_rows(selected_rows: list[dict], candidate_set: list[dict]) -> list[dict]:
    rows: list[dict] = []
    seen: set[tuple] = set()
    for row in selected_rows + [
        item
        for candidate in candidate_set
        for item in candidate["rows"]
    ]:
        key = _prediction_row_key(row)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)
    return rows


def _candidate_selection_key(candidates: list[dict], metrics: dict) -> tuple:
    return (
        int(_safe_num(metrics.get("prediction_count"), 0)),
        _safe_num(metrics.get("accuracy"), -1.0),
        _safe_num(metrics.get("reversal_accuracy"), -1.0),
        _safe_num(metrics.get("continuation_accuracy"), -1.0),
        _safe_num(metrics.get("expected_return"), -999.0),
        _safe_num(metrics.get("atr_reward_risk"), -1.0),
        sum(1 for candidate in candidates if candidate["gate"].get("direction") == "reversal"),
    )


def _candidate_priority_key(candidate: dict) -> tuple:
    return (
        int(_safe_num(candidate["metrics"].get("prediction_count"), 0)),
        _safe_num(candidate["metrics"].get("accuracy"), 0.0),
        _safe_num(candidate["metrics"].get("expected_return"), -999.0),
        _safe_num(candidate["metrics"].get("atr_reward_risk"), -1.0),
        1 if candidate["gate"].get("direction") == "reversal" else 0,
    )


def _select_max_safe_coverage_candidates(selected_rows: list[dict], candidates: list[dict]) -> list[dict]:
    baseline_metrics = _prediction_book_metrics(selected_rows)

    def candidate_is_safe(candidate_set: list[dict]) -> tuple[bool, dict]:
        rows = _candidate_rows(selected_rows, candidate_set)
        metrics = _prediction_book_metrics(rows)
        return (
            _prediction_book_passes_quality(metrics)
            and _prediction_book_preserves_accuracy(baseline_metrics, metrics),
            metrics,
        )

    if len(candidates) <= _MAX_EXACT_COVERAGE_EXPANSION_CANDIDATES:
        best_candidates: list[dict] = []
        best_key = _candidate_selection_key([], baseline_metrics)
        for mask in range(1, 1 << len(candidates)):
            candidate_set = [
                candidate
                for index, candidate in enumerate(candidates)
                if mask & (1 << index)
            ]
            is_safe, metrics = candidate_is_safe(candidate_set)
            if not is_safe:
                continue
            key = _candidate_selection_key(candidate_set, metrics)
            if key > best_key:
                best_key = key
                best_candidates = candidate_set
        return best_candidates

    remaining = sorted(candidates, key=_candidate_priority_key, reverse=True)
    chosen: list[dict] = []
    while remaining:
        current_rows = _candidate_rows(selected_rows, chosen)
        current_metrics = _prediction_book_metrics(current_rows)
        current_key = _candidate_selection_key(chosen, current_metrics)
        safe_candidates: list[tuple[tuple, dict]] = []
        for candidate in remaining:
            is_safe, metrics = candidate_is_safe(chosen + [candidate])
            if is_safe:
                key = _candidate_selection_key(chosen + [candidate], metrics)
                if key > current_key:
                    safe_candidates.append((key, candidate))
        if not safe_candidates:
            break
        _, next_candidate = max(safe_candidates, key=lambda item: item[0])
        chosen.append(next_candidate)
        remaining = [candidate for candidate in remaining if candidate is not next_candidate]
    return chosen


def _apply_max_safe_coverage_to_signal_gates(backtest: dict, signal_gates: dict[str, dict]) -> None:
    eligible_count = int(_safe_num(backtest.get("eligible_touch_count"), 0))
    if eligible_count <= 0:
        backtest["coverage_policy"] = _COVERAGE_POLICY_MAX_SAFE
        return

    predictions = list(backtest.get("predictions", []))
    if not predictions:
        backtest["coverage_policy"] = _COVERAGE_POLICY_MAX_SAFE
        backtest["max_safe_prediction_count"] = 0
        backtest["max_safe_coverage"] = 0.0
        return

    selected = [
        item
        for item in predictions
        if signal_gates.get(_prediction_group_key(item), {}).get("deployment_enabled", False)
    ]

    groups: dict[str, list[dict]] = {}
    for item in predictions:
        key = _prediction_group_key(item)
        if signal_gates.get(key, {}).get("deployment_enabled", False):
            continue
        groups.setdefault(key, []).append(item)

    candidates: list[dict] = []
    for key, rows in groups.items():
        metrics = _prediction_book_metrics(rows)
        group_accuracy = _safe_num(metrics.get("accuracy"), 0.0)
        if group_accuracy < _COVERAGE_EXPANSION_MIN_SIGNAL_ACCURACY:
            continue
        gate = signal_gates.get(key, {})
        candidates.append(
            {
                "key": key,
                "rows": rows,
                "metrics": metrics,
                "gate": gate,
                "accuracy": group_accuracy,
            }
        )

    selected_candidates = _select_max_safe_coverage_candidates(selected, candidates)
    selected_keys = [candidate["key"] for candidate in selected_candidates]
    final_rows = selected + [
        row
        for selected_candidate in selected_candidates
        for row in selected_candidate["rows"]
    ]
    final_metrics = _prediction_book_metrics(final_rows)

    for candidate in selected_candidates:
        gate = signal_gates[candidate["key"]]
        gate["status"] = "coverage_expansion"
        gate["deployment_enabled"] = True
        gate["coverage_expansion"] = True
        gate["coverage_policy"] = _COVERAGE_POLICY_MAX_SAFE
        gate["portfolio_accuracy_after_expansion"] = round(_safe_num(final_metrics.get("accuracy")), 6)
        gate["portfolio_reverse_accuracy_after_expansion"] = (
            round(_safe_num(final_metrics.get("reversal_accuracy")), 6)
            if final_metrics.get("reversal_accuracy") is not None
            else None
        )
        gate["portfolio_continue_accuracy_after_expansion"] = (
            round(_safe_num(final_metrics.get("continuation_accuracy")), 6)
            if final_metrics.get("continuation_accuracy") is not None
            else None
        )
        gate["failures"] = []

    backtest["coverage_policy"] = _COVERAGE_POLICY_MAX_SAFE
    backtest["max_safe_prediction_count"] = int(_safe_num(final_metrics.get("prediction_count"), 0))
    backtest["max_safe_coverage"] = round(len(final_rows) / eligible_count, 6)
    if selected_keys:
        backtest["coverage_expansion_signal_count"] = len(selected_keys)


def _coverage_repair_specs() -> list[tuple[str, tuple[str, ...], int]]:
    return [
        (
            "blocked_side_touch_cluster_trend",
            ("side", "blocked_direction", "touch_quality", "cluster", "trend"),
            3,
        ),
        ("blocked_side_touch_trend", ("side", "blocked_direction", "touch_quality", "trend"), 3),
        ("blocked_side_cluster_trend", ("side", "blocked_direction", "cluster", "trend"), 3),
        ("blocked_side_reason_trend", ("side", "blocked_direction", "reason", "trend"), 3),
        ("blocked_side_trend_market", ("side", "blocked_direction", "trend", "market"), 3),
        ("blocked_side_touch_band", ("side", "blocked_direction", "touch_quality", "band_state"), 3),
        ("blocked_side_trend", ("side", "blocked_direction", "trend"), 4),
        ("blocked_side_reason", ("side", "blocked_direction", "reason"), 4),
        ("blocked_side", ("side", "blocked_direction"), 6),
        ("side_touch_cluster_trend", ("side", "touch_quality", "cluster", "trend"), 3),
        ("side_touch_trend_market", ("side", "touch_quality", "trend", "market"), 3),
        ("side_touch_volume", ("side", "touch_quality", "volume_pressure"), 4),
        ("side_trend_market", ("side", "trend", "market"), 4),
        ("side_touch_band", ("side", "touch_quality", "band_state"), 4),
        ("side_touch_trend", ("side", "touch_quality", "trend"), 3),
        ("side_cluster_trend", ("side", "cluster", "trend"), 3),
        ("side_reason_trend", ("side", "reason", "trend"), 3),
        ("side_touch_cluster", ("side", "touch_quality", "cluster"), 4),
        ("side_band_state", ("side", "band_state"), 5),
        ("side_volume_pressure", ("side", "volume_pressure"), 5),
        ("side_trend", ("side", "trend"), 4),
        ("side_cluster", ("side", "cluster"), 4),
        ("side_reason", ("side", "reason"), 4),
        ("side_only", ("side",), 8),
    ]


def _coverage_repair_attrs(row: pd.Series, horizon_decision: dict, horizon: int) -> dict[str, str]:
    attrs = _empirical_regime_attrs(row, horizon)
    attrs["reason"] = str(horizon_decision.get("no_prediction_reason") or "unknown")
    blocked = horizon_decision.get("blocked_prediction") or {}
    attrs["blocked_direction"] = str(blocked.get("predicted_direction") or "none")
    return attrs


def _coverage_repair_policy_id(scope: str, direction: str, fields: tuple[str, ...], attrs: dict[str, str]) -> str:
    pieces = [scope, direction] + [f"{field}_{attrs.get(field, 'na')}" for field in fields]
    text = "_".join(pieces).lower()
    return "coverage_repair_" + re.sub(r"[^a-z0-9]+", "_", text).strip("_")


def _coverage_repair_policy_score(
    precision: float,
    posterior: float,
    wilson: float,
    match_count: int,
    field_count: int,
) -> float:
    sample_strength = min(1.0, math.log1p(max(0, match_count)) / math.log1p(32.0))
    specificity = min(1.0, max(0, field_count) / 5.0)
    return (
        (0.40 * precision)
        + (0.24 * wilson)
        + (0.18 * posterior)
        + (0.12 * sample_strength)
        + (0.06 * specificity)
    )


def _coverage_repair_calibrated_probability(precision: float, posterior: float, wilson: float) -> float:
    return _clamp(
        (0.55 * posterior) + (0.30 * precision) + (0.15 * wilson),
        0.51,
        0.99,
    )


def _coverage_repair_policy_rank_key(policy: dict) -> tuple:
    return (
        _safe_num(policy.get("score"), 0.0),
        _safe_num(policy.get("wilson_lower_bound"), 0.0),
        _safe_num(policy.get("posterior_probability"), 0.0),
        _safe_num(policy.get("precision"), 0.0),
        int(_safe_num(policy.get("match_count"), 0)),
        len(policy.get("fields") or []),
    )


def _coverage_repair_prediction_row(
    feature_df: pd.DataFrame,
    idx: int,
    horizon: int,
    direction: str,
    actual_direction: str,
    policy: dict,
) -> dict:
    row = feature_df.iloc[idx]
    signal_close = _safe_num(row.get("close"), np.nan)
    outcome_row = feature_df.iloc[idx + horizon]
    outcome_close = _safe_num(outcome_row.get("close"), np.nan)
    atr = _safe_num(row.get("ATR14"), np.nan)
    return_values = _prediction_return_values(
        row.get("touched_side"),
        direction,
        signal_close,
        outcome_close,
        atr,
    )
    confidence = _safe_num(policy.get("calibrated_probability"), policy.get("posterior_probability", 0.0))
    return {
        "_repair_row_key": (int(idx), horizon),
        "_repair_policy": policy,
        "signal_date": _to_date_string(row.get("date")),
        "outcome_date": _to_date_string(outcome_row.get("date")),
        "horizon_days": horizon,
        "touched_side": row.get("touched_side"),
        "predicted_direction": direction,
        "actual_direction": actual_direction,
        "confidence_score": int(round(confidence * 100.0)),
        "is_correct": _prediction_is_correct(direction, actual_direction),
        "signal_close": round(signal_close, 6) if np.isfinite(signal_close) else None,
        "outcome_close": round(outcome_close, 6) if np.isfinite(outcome_close) else None,
        "flat_tolerance": round(_flat_tolerance_for_row(row), 6),
        **return_values,
        "signal_model": "Selective Coverage Repair",
        "signal_model_id": policy["id"],
        "signal_precision": policy["precision"],
        "signal_tier": "coverage_repair",
    }


def _coverage_repair_similar_cases(prediction_rows: list[dict], limit: int = 8) -> list[dict]:
    cases: list[dict] = []
    for row in prediction_rows[-limit:]:
        cases.append(
            {
                "signal_date": row.get("signal_date"),
                "outcome_date": row.get("outcome_date"),
                "horizon_days": row.get("horizon_days"),
                "touched_side": row.get("touched_side"),
                "predicted_direction": row.get("predicted_direction"),
                "actual_direction": row.get("actual_direction"),
                "signal_close": row.get("signal_close"),
                "outcome_close": row.get("outcome_close"),
                "is_correct": row.get("is_correct"),
                "trade_direction": row.get("trade_direction"),
                "trade_return": row.get("trade_return"),
                "trade_return_atr": row.get("trade_return_atr"),
            }
        )
    return cases


def _coverage_repair_candidates(
    feature_df: pd.DataFrame,
    decisions_by_index: dict[int, dict],
    horizon: int,
) -> list[dict]:
    if feature_df.empty:
        return []

    horizon_key = f"{horizon}d"
    last_date = pd.Timestamp(feature_df.iloc[-1].get("date")).normalize()
    period_start = max(
        pd.Timestamp(feature_df.iloc[0].get("date")).normalize(),
        last_date - pd.DateOffset(days=_BACKTEST_LOOKBACK_DAYS),
    )
    groups: dict[tuple, list[dict]] = {}

    for idx, decision in decisions_by_index.items():
        if idx + horizon >= len(feature_df):
            continue
        row = feature_df.iloc[idx]
        signal_date = pd.Timestamp(row.get("date")).normalize()
        if signal_date < period_start:
            continue
        if _safe_bool(row.get("event_risk_blocked")):
            continue
        if row.get("touched_side") not in ("Upper", "Lower"):
            continue
        horizon_decision = decision.get("horizons", {}).get(horizon_key) or {}
        if horizon_decision.get("status") == "prediction":
            continue
        actual = _actual_direction_for_index(feature_df, idx, horizon)
        if actual not in ("continuation", "reversal"):
            continue
        attrs = _coverage_repair_attrs(row, horizon_decision, horizon)
        for scope, fields, min_matches in _coverage_repair_specs():
            key = (scope, fields, tuple((field, attrs.get(field)) for field in fields), min_matches)
            groups.setdefault(key, []).append(
                {
                    "idx": int(idx),
                    "actual_direction": actual,
                    "attrs": attrs,
                }
            )

    candidates: list[dict] = []
    for (scope, fields, field_values, min_matches), rows in groups.items():
        if len(rows) < max(min_matches, _COVERAGE_REPAIR_MIN_MATCHES):
            continue
        attrs = dict(field_values)
        for direction in ("reversal", "continuation"):
            correct_count = sum(1 for row in rows if row["actual_direction"] == direction)
            match_count = len(rows)
            precision = correct_count / match_count
            posterior = (correct_count + 1.0) / (match_count + 2.0)
            wilson = _wilson_lower_bound(correct_count, match_count)
            if (
                precision < _COVERAGE_REPAIR_MIN_PRECISION
                or wilson < _COVERAGE_REPAIR_MIN_WILSON
            ):
                continue
            score = _coverage_repair_policy_score(
                precision,
                posterior,
                wilson,
                match_count,
                len(fields),
            )
            calibrated_probability = _coverage_repair_calibrated_probability(precision, posterior, wilson)
            policy = {
                "id": _coverage_repair_policy_id(scope, direction, fields, attrs),
                "name": "Selective Coverage Repair",
                "direction": direction,
                "tier": "coverage_repair",
                "scope": scope,
                "fields": list(fields),
                "attrs": attrs,
                "match_count": match_count,
                "correct_count": correct_count,
                "precision": round(precision, 6),
                "posterior_probability": round(posterior, 6),
                "wilson_lower_bound": round(wilson, 6),
                "calibrated_probability": round(calibrated_probability, 6),
                "score": round(score, 6),
            }
            prediction_rows = [
                _coverage_repair_prediction_row(
                    feature_df,
                    row["idx"],
                    horizon,
                    direction,
                    row["actual_direction"],
                    policy,
                )
                for row in rows
            ]
            policy["neighbors"] = _coverage_repair_similar_cases(prediction_rows)
            candidates.append(
                {
                    "key": policy["id"],
                    "rows": prediction_rows,
                    "metrics": _prediction_book_metrics(prediction_rows),
                    "gate": {"direction": direction},
                    "policy": policy,
                }
            )
    return candidates


def _coverage_repair_policy_matches(row: pd.Series, horizon_decision: dict, horizon: int, policy: dict) -> bool:
    attrs = _coverage_repair_attrs(row, horizon_decision, horizon)
    return all(str(attrs.get(field)) == str(policy.get("attrs", {}).get(field)) for field in policy.get("fields", []))


def _coverage_repair_horizon(
    feature_df: pd.DataFrame,
    idx: int,
    horizon: int,
    policy: dict,
) -> dict:
    direction = policy["direction"]
    precision = _clamp(_safe_num(policy.get("precision"), 0.5), 0.01, 0.99)
    posterior = _clamp(_safe_num(policy.get("posterior_probability"), precision), 0.01, 0.99)
    wilson = _clamp(_safe_num(policy.get("wilson_lower_bound"), posterior), 0.01, 0.99)
    probability = _clamp(
        _safe_num(
            policy.get("calibrated_probability"),
            _coverage_repair_calibrated_probability(precision, posterior, wilson),
        ),
        0.01,
        0.99,
    )
    if direction == "continuation":
        continuation_probability = probability
        reversal_probability = 1.0 - probability
    else:
        reversal_probability = probability
        continuation_probability = 1.0 - probability

    training_count, continuation_count, reversal_count = _adaptive_training_summary(feature_df, idx, horizon)
    model = _model_metadata(
        training_count,
        continuation_count,
        reversal_count,
        validation_accuracy=precision,
        validation_precision=precision,
        candidate_count=1,
    )
    model["type"] = "selective_coverage_repair"
    reason = {
        "rank": 1,
        "horizon": f"{horizon}d",
        "feature": policy["name"],
        "value": policy["match_count"],
        "impact": direction,
        "contribution": policy["precision"],
    }
    return {
        "status": "prediction",
        "predicted_direction": direction,
        "continuation_probability": round(continuation_probability, 6),
        "reversal_probability": round(reversal_probability, 6),
        "continuation_confidence_score": int(round(continuation_probability * 100)),
        "reversal_confidence_score": int(round(reversal_probability * 100)),
        "continuation_validation_precision": precision if direction == "continuation" else None,
        "reversal_validation_precision": precision if direction == "reversal" else None,
        "continuation_validation_count": policy["match_count"] if direction == "continuation" else 0,
        "reversal_validation_count": policy["match_count"] if direction == "reversal" else 0,
        "validation_policy": {
            "coverage_policy": _COVERAGE_POLICY_MAX_SAFE,
            "min_precision": _COVERAGE_REPAIR_MIN_PRECISION,
            "min_wilson_lower_bound": _COVERAGE_REPAIR_MIN_WILSON,
            "scope": policy["scope"],
            "fields": policy["fields"],
            "attrs": policy["attrs"],
        },
        "confidence_score": int(round(max(continuation_probability, reversal_probability) * 100)),
        "threshold": _COVERAGE_REPAIR_MIN_PRECISION,
        "no_prediction_reason": None,
        "reversal_veto_reason": None,
        "analog_evidence": {
            "status": "ready",
            "direction": direction,
            "posterior_probability": policy["posterior_probability"],
            "neighbor_count": policy["match_count"],
            "precision": policy["precision"],
            "lower_bound": policy["wilson_lower_bound"],
        },
        "analog_override": False,
        "deployment_quality_gate": {
            "status": "coverage_repair",
            "deployment_enabled": True,
            "coverage_policy": _COVERAGE_POLICY_MAX_SAFE,
        },
        "blocked_prediction": None,
        "playbook": deepcopy(policy),
        "adaptive_candidates": [
            {
                "profile_id": policy["id"],
                "status": "ready",
                "direction": direction,
                "tier": "coverage_repair",
                "precision": policy["precision"],
                "validation_count": policy["match_count"],
                "confidence": round(max(continuation_probability, reversal_probability), 6),
                "blocked_reason": None,
            }
        ],
        "key_reasons": [deepcopy(reason)],
        "similar_past_cases": deepcopy(policy.get("neighbors") or []),
        "contributions": [reason],
        "model": model,
    }


def _apply_coverage_repair_policies(
    feature_df: pd.DataFrame,
    decisions_by_index: dict[int, dict],
    policies_by_horizon: dict[str, list[dict]],
) -> None:
    for idx, decision in decisions_by_index.items():
        row = feature_df.iloc[idx]
        if row.get("touched_side") not in ("Upper", "Lower"):
            continue
        if _safe_bool(row.get("event_risk_blocked")):
            continue
        for horizon in _HORIZONS:
            horizon_key = f"{horizon}d"
            horizon_decision = decision.get("horizons", {}).get(horizon_key) or {}
            if horizon_decision.get("status") == "prediction":
                continue
            matching = [
                policy
                for policy in policies_by_horizon.get(horizon_key, [])
                if _coverage_repair_policy_matches(row, horizon_decision, horizon, policy)
            ]
            if not matching:
                continue
            policy = sorted(
                matching,
                key=_coverage_repair_policy_rank_key,
                reverse=True,
            )[0]
            decision["horizons"][horizon_key] = _coverage_repair_horizon(feature_df, idx, horizon, policy)


def _coverage_repair_policies_by_horizon(backtest_1y: dict) -> dict[str, list[dict]]:
    policies_by_horizon: dict[str, list[dict]] = {}
    for horizon_key, result in backtest_1y.items():
        if isinstance(result, dict):
            policies_by_horizon[horizon_key] = deepcopy(result.get("coverage_repair_policies") or [])
    return policies_by_horizon


def _direction_gate_blocked_horizon(horizon_key: str, original: dict, gate: dict) -> dict:
    horizon = _horizon_from_key(horizon_key)
    model_meta = original.get("model", {}) if isinstance(original, dict) else {}
    blocked = _no_prediction_horizon(
        horizon,
        "direction_quality_gate_failed",
        training_sample_count=int(_safe_num(model_meta.get("training_sample_count"), 0)),
        positive_count=int(_safe_num(model_meta.get("continuation_training_count"), 0)),
        negative_count=int(_safe_num(model_meta.get("reversal_training_count"), 0)),
    )
    blocked["deployment_quality_gate"] = deepcopy(gate)
    blocked["blocked_prediction"] = {
        "status": original.get("status"),
        "predicted_direction": original.get("predicted_direction"),
        "continuation_probability": original.get("continuation_probability"),
        "reversal_probability": original.get("reversal_probability"),
        "confidence_score": original.get("confidence_score"),
        "no_prediction_reason": original.get("no_prediction_reason"),
    }
    blocked["model"] = deepcopy(model_meta) if model_meta else blocked["model"]
    return blocked


def _missing_signal_quality_gate(direction: str | None, signal_id: str | None, direction_gate: dict | None) -> dict:
    min_accuracy = (
        _DEPLOY_MIN_BACKTEST_CONTINUE_ACCURACY
        if direction == "continuation"
        else _DEPLOY_MIN_BACKTEST_REVERSE_ACCURACY
    )
    min_wilson = _signal_min_wilson(direction or "reversal")
    return {
        "status": "quarantined",
        "deployment_enabled": False,
        "direction": direction,
        "signal_id": signal_id or "unknown",
        "failures": ["missing_signal_backtest"],
        "min_prediction_count": _DEPLOY_MIN_SIGNAL_BACKTEST_CALLS,
        "min_accuracy": min_accuracy,
        "min_wilson_lower_bound": min_wilson,
        "wilson_z_value": _DEPLOYMENT_WILSON_Z,
        "min_expected_return": _DEPLOY_MIN_SIGNAL_EXPECTED_RETURN,
        "raw_prediction_count": 0,
        "raw_correct_count": 0,
        "raw_accuracy": None,
        "raw_wilson_lower_bound": None,
        "raw_expected_return": None,
        "direction_gate_status": (direction_gate or {}).get("status"),
    }


def _apply_direction_quality_gates_to_decision(
    decision: dict,
    gates: dict[str, dict],
    *,
    signal_date: Any = None,
) -> dict:
    if not decision:
        return decision
    horizons = decision.get("horizons", {})
    for horizon_key, horizon_decision in list(horizons.items()):
        if not isinstance(horizon_decision, dict):
            continue
        direction = horizon_decision.get("predicted_direction")
        horizon_gate = gates.get(horizon_key, {})
        gate_container = horizon_gate.get("direction_quality_gate", horizon_gate)
        signal_key = _signal_gate_key(direction, _prediction_signal_id_from_horizon(horizon_decision))
        signal_gate = (gate_container.get("signals") or {}).get(signal_key)
        direction_gate = gate_container.get(direction) if direction in ("continuation", "reversal") else None
        if horizon_decision.get("status") != "prediction" or not isinstance(direction_gate, dict):
            if horizon_decision.get("deployment_quality_gate") is None:
                horizon_decision["deployment_quality_gate"] = deepcopy(gate_container) if gate_container else None
            continue
        active_gate = signal_gate
        raw_predictions = horizon_gate.get("_raw_predictions_for_gate") if isinstance(horizon_gate, dict) else None
        if raw_predictions and signal_date is not None:
            active_gate = _deployment_gate_for_horizon_decision(
                decision,
                horizon_key,
                horizon_decision,
                raw_predictions,
                signal_date,
            )
        if active_gate is None:
            active_gate = _missing_signal_quality_gate(
                direction,
                _prediction_signal_id_from_horizon(horizon_decision),
                direction_gate,
            )
        if not active_gate.get("deployment_enabled", False):
            horizons[horizon_key] = _direction_gate_blocked_horizon(horizon_key, horizon_decision, active_gate)
        else:
            horizon_decision["deployment_quality_gate"] = deepcopy(active_gate)
    return decision


def _apply_deployment_quality_gates_to_decision(decision: dict, gates: dict[str, dict]) -> dict:
    if not decision:
        return decision
    horizons = decision.get("horizons", {})
    for horizon_key, gate in gates.items():
        if gate.get("deployment_enabled", True):
            if horizon_key in horizons and isinstance(horizons[horizon_key], dict):
                horizons[horizon_key]["deployment_quality_gate"] = deepcopy(gate)
            continue
        original = horizons.get(horizon_key)
        if not isinstance(original, dict):
            continue
        horizons[horizon_key] = _quality_gate_blocked_horizon(horizon_key, original, gate)
    return decision


def _finalize_deployment_quality(
    feature_df: pd.DataFrame,
    decisions_by_index: dict[int, dict],
) -> tuple[dict[int, dict], dict]:
    raw_backtest = run_decision_backtest(feature_df, decisions_by_index=decisions_by_index)
    evidence_backtest = run_decision_backtest(
        feature_df,
        decisions_by_index=decisions_by_index,
        lookback_days=_DEPLOYMENT_EVIDENCE_LOOKBACK_DAYS,
    )
    direction_gates = _direction_quality_gates(raw_backtest)

    # Report the deployable walk-forward tape: each historical prediction is
    # counted only when its signal family had already proven itself on prior
    # completed outcomes before that signal date. The evidence window is longer
    # than the report window, but every gate still filters by outcome_date <
    # signal_date, so older rows can help and future rows cannot.
    final_backtest = {
        horizon_key: _deployment_backtest_from_raw(
            result,
            evidence_backtest.get(horizon_key, {}),
        )
        for horizon_key, result in raw_backtest.items()
        if isinstance(result, dict)
    }
    for horizon_key, result in final_backtest.items():
        raw_result = raw_backtest.get(horizon_key, {})
        evidence_result = evidence_backtest.get(horizon_key, {})
        result["raw_prediction_count"] = raw_result.get("prediction_count")
        result["raw_accuracy"] = raw_result.get("accuracy")
        result["raw_reverse_accuracy"] = raw_result.get("reversal_accuracy")
        result["raw_continue_accuracy"] = raw_result.get("continuation_accuracy")
        result["raw_win_rate"] = raw_result.get("win_rate")
        result["raw_expected_return"] = raw_result.get("expected_return")
        result["raw_expected_downside"] = raw_result.get("expected_downside")
        result["raw_expected_atr_return"] = raw_result.get("expected_atr_return")
        result["raw_expected_atr_downside"] = raw_result.get("expected_atr_downside")
        result["raw_atr_reward_risk"] = raw_result.get("atr_reward_risk")
        result["raw_coverage"] = raw_result.get("coverage")
        result["quality_gate"] = _deployment_quality_gate(result)
        result["direction_quality_gate"] = deepcopy(direction_gates.get(horizon_key, {}))
        result["_raw_predictions_for_gate"] = deepcopy(evidence_result.get("predictions", []))
        result["deployment_evidence_lookback_days"] = _DEPLOYMENT_EVIDENCE_LOOKBACK_DAYS
        result["deployment_evidence_prediction_count"] = len(evidence_result.get("predictions", []))
        result["deployment_evidence_period_start"] = evidence_result.get("period_start")
        result["deployment_evidence_period_end"] = evidence_result.get("period_end")
        result["raw_recent_predictions"] = deepcopy(raw_result.get("recent_predictions", []))
        result["raw_backtest_policy"] = _RAW_BACKTEST_REPORTING_POLICY
        result["coverage_policy"] = _BACKTEST_REPORTING_POLICY
        result["backtest_policy"] = _BACKTEST_REPORTING_POLICY
        result["max_safe_prediction_count"] = result.get("prediction_count")
        result["max_safe_coverage"] = result.get("coverage")
        result["coverage_expansion_signal_count"] = 0
        result["coverage_repair_policy_count"] = 0
        result["coverage_repair_prediction_count"] = 0
        result["coverage_repair_policies"] = []
    return decisions_by_index, final_backtest




__all__ = [name for name in globals() if not name.startswith("__")]
