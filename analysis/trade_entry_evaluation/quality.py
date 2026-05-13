from __future__ import annotations

from .settings import *
from .features import *
from .model import *
from .backtest import *
from .adaptive import _adaptive_training_summary, _empirical_regime_attrs
from .playbooks import _actual_direction_for_index

def _deployment_quality_gate(backtest: dict) -> dict:
    prediction_count = int(_safe_num(backtest.get("prediction_count"), 0))
    failures: list[str] = []

    if prediction_count == 0:
        return {
            "status": "idle",
            "deployment_enabled": True,
            "failures": [],
            "min_prediction_count": _DEPLOY_MIN_BACKTEST_CALLS,
            "min_accuracy": _DEPLOY_MIN_BACKTEST_ACCURACY,
            "min_reverse_accuracy": _DEPLOY_MIN_BACKTEST_REVERSE_ACCURACY,
            "min_continue_accuracy": _DEPLOY_MIN_BACKTEST_CONTINUE_ACCURACY,
            "raw_prediction_count": 0,
            "raw_accuracy": None,
            "raw_reverse_accuracy": None,
            "raw_continue_accuracy": None,
        }

    accuracy = backtest.get("accuracy")
    reversal_accuracy = backtest.get("reversal_accuracy")
    continuation_accuracy = backtest.get("continuation_accuracy")
    reversal_call_count = int(_safe_num(backtest.get("reversal_call_count"), 0))
    continuation_call_count = int(_safe_num(backtest.get("continuation_call_count"), 0))

    if prediction_count < _DEPLOY_MIN_BACKTEST_CALLS:
        failures.append("insufficient_deployed_sample")
    if accuracy is None or _safe_num(accuracy, 0.0) < _DEPLOY_MIN_BACKTEST_ACCURACY:
        failures.append("weak_deployed_accuracy")
    if reversal_call_count > 0 and (
        reversal_accuracy is None or _safe_num(reversal_accuracy, 0.0) < _DEPLOY_MIN_BACKTEST_REVERSE_ACCURACY
    ):
        failures.append("weak_reverse_accuracy")
    if continuation_call_count > 0 and (
        continuation_accuracy is None or _safe_num(continuation_accuracy, 0.0) < _DEPLOY_MIN_BACKTEST_CONTINUE_ACCURACY
    ):
        failures.append("weak_continue_accuracy")

    return {
        "status": "quarantined" if failures else "passed",
        "deployment_enabled": not failures,
        "failures": failures,
        "min_prediction_count": _DEPLOY_MIN_BACKTEST_CALLS,
        "min_accuracy": _DEPLOY_MIN_BACKTEST_ACCURACY,
        "min_reverse_accuracy": _DEPLOY_MIN_BACKTEST_REVERSE_ACCURACY,
        "min_continue_accuracy": _DEPLOY_MIN_BACKTEST_CONTINUE_ACCURACY,
        "raw_prediction_count": prediction_count,
        "raw_accuracy": accuracy,
        "raw_reverse_accuracy": reversal_accuracy,
        "raw_continue_accuracy": continuation_accuracy,
    }


def _horizon_from_key(horizon_key: str) -> int:
    try:
        return int(str(horizon_key).lower().replace("d", ""))
    except (TypeError, ValueError):
        return 0


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
        accuracy = backtest.get("continuation_accuracy")
        min_accuracy = _DEPLOY_MIN_BACKTEST_CONTINUE_ACCURACY
    else:
        call_count = int(_safe_num(backtest.get("reversal_call_count"), 0))
        accuracy = backtest.get("reversal_accuracy")
        min_accuracy = _DEPLOY_MIN_BACKTEST_REVERSE_ACCURACY

    failures: list[str] = []
    if call_count < _DEPLOY_MIN_BACKTEST_CALLS:
        failures.append("insufficient_direction_sample")
    if accuracy is None or _safe_num(accuracy, 0.0) < min_accuracy:
        failures.append(f"weak_{direction}_accuracy")

    return {
        "status": "quarantined" if failures else "passed",
        "deployment_enabled": not failures,
        "direction": direction,
        "failures": failures,
        "min_prediction_count": _DEPLOY_MIN_BACKTEST_CALLS,
        "min_accuracy": min_accuracy,
        "raw_prediction_count": call_count,
        "raw_accuracy": accuracy,
    }


def _direction_quality_gates(backtest_by_horizon: dict) -> dict[str, dict]:
    gates: dict[str, dict] = {}
    for horizon_key, result in backtest_by_horizon.items():
        if not isinstance(result, dict):
            continue
        continuation_gate = _direction_quality_gate(result, "continuation")
        reversal_gate = _direction_quality_gate(result, "reversal")
        signal_gates = _signal_quality_gates_for_horizon(result, continuation_gate, reversal_gate)
        _apply_max_safe_coverage_to_signal_gates(result, signal_gates)
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


def _signal_gate_key(direction: str | None, signal_id: str | None) -> str:
    return f"{direction or 'unknown'}:{signal_id or 'unknown'}"


def _prediction_signal_id_from_backtest_row(row: dict) -> str:
    return str(row.get("signal_model_id") or row.get("signal_tier") or "unknown")


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
            },
        )
        bucket["prediction_count"] += 1
        if prediction.get("is_correct"):
            bucket["correct_count"] += 1

    gates: dict[str, dict] = {}
    for key, bucket in grouped.items():
        direction = bucket["direction"]
        direction_gate = continuation_gate if direction == "continuation" else reversal_gate
        min_accuracy = (
            _DEPLOY_MIN_BACKTEST_CONTINUE_ACCURACY
            if direction == "continuation"
            else _DEPLOY_MIN_BACKTEST_REVERSE_ACCURACY
        )
        prediction_count = int(bucket["prediction_count"])
        correct_count = int(bucket["correct_count"])
        accuracy = correct_count / prediction_count if prediction_count else None
        failures: list[str] = []
        inherited_direction_pass = bool(direction_gate.get("deployment_enabled"))

        if prediction_count < _DEPLOY_MIN_SIGNAL_BACKTEST_CALLS and not inherited_direction_pass:
            failures.append("insufficient_signal_sample")
        if accuracy is None or _safe_num(accuracy, 0.0) < min_accuracy:
            failures.append(f"weak_{direction}_signal_accuracy")
        if inherited_direction_pass and prediction_count < _DEPLOY_MIN_SIGNAL_BACKTEST_CALLS:
            failures = [failure for failure in failures if failure != "insufficient_signal_sample"]

        gates[key] = {
            "status": "quarantined" if failures else "passed",
            "deployment_enabled": not failures,
            "direction": direction,
            "signal_id": bucket["signal_id"],
            "failures": failures,
            "min_prediction_count": _DEPLOY_MIN_SIGNAL_BACKTEST_CALLS,
            "min_accuracy": min_accuracy,
            "raw_prediction_count": prediction_count,
            "raw_correct_count": correct_count,
            "raw_accuracy": round(accuracy, 6) if accuracy is not None else None,
            "direction_gate_status": direction_gate.get("status"),
        }
    return gates


def _prediction_group_key(prediction: dict) -> str:
    return _signal_gate_key(
        prediction.get("predicted_direction"),
        _prediction_signal_id_from_backtest_row(prediction),
    )


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
        safe_candidates: list[tuple[tuple, dict]] = []
        for candidate in remaining:
            is_safe, metrics = candidate_is_safe(chosen + [candidate])
            if is_safe:
                safe_candidates.append((_candidate_selection_key(chosen + [candidate], metrics), candidate))
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
        ("side_touch_cluster_trend", ("side", "touch_quality", "cluster", "trend"), 3),
        ("side_touch_trend", ("side", "touch_quality", "trend"), 3),
        ("side_cluster_trend", ("side", "cluster", "trend"), 3),
        ("side_reason_trend", ("side", "reason", "trend"), 3),
        ("side_touch_cluster", ("side", "touch_quality", "cluster"), 4),
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
    return {
        "_repair_row_key": (int(idx), horizon),
        "_repair_policy": policy,
        "signal_date": _to_date_string(row.get("date")),
        "outcome_date": _to_date_string(outcome_row.get("date")),
        "horizon_days": horizon,
        "touched_side": row.get("touched_side"),
        "predicted_direction": direction,
        "actual_direction": actual_direction,
        "confidence_score": int(round(_safe_num(policy.get("precision"), 0.0) * 100.0)),
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
            wilson = _wilson_lower_bound(correct_count, match_count)
            if (
                precision < _COVERAGE_REPAIR_MIN_PRECISION
                or wilson < _COVERAGE_REPAIR_MIN_WILSON
            ):
                continue
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
                "posterior_probability": round((correct_count + 1.0) / (match_count + 2.0), 6),
                "wilson_lower_bound": round(wilson, 6),
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
    probability = max(precision, posterior)
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
        "contributions": [
            {
                "horizon": f"{horizon}d",
                "feature": policy["name"],
                "value": policy["match_count"],
                "impact": direction,
                "contribution": policy["precision"],
            }
        ],
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
                key=lambda item: (item["precision"], item["match_count"], len(item["fields"])),
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


def _apply_direction_quality_gates_to_decision(decision: dict, gates: dict[str, dict]) -> dict:
    if not decision:
        return decision
    horizons = decision.get("horizons", {})
    for horizon_key, horizon_decision in list(horizons.items()):
        if not isinstance(horizon_decision, dict):
            continue
        direction = horizon_decision.get("predicted_direction")
        horizon_gate = gates.get(horizon_key, {})
        signal_key = _signal_gate_key(direction, _prediction_signal_id_from_horizon(horizon_decision))
        signal_gate = (horizon_gate.get("signals") or {}).get(signal_key)
        direction_gate = horizon_gate.get(direction) if direction in ("continuation", "reversal") else None
        active_gate = signal_gate or direction_gate
        if horizon_decision.get("status") != "prediction" or not isinstance(direction_gate, dict):
            horizon_decision["deployment_quality_gate"] = deepcopy(horizon_gate) if horizon_gate else None
            continue
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
    direction_gates = _direction_quality_gates(raw_backtest)

    gated_decisions = deepcopy(decisions_by_index)
    for decision in gated_decisions.values():
        _apply_direction_quality_gates_to_decision(decision, direction_gates)

    baseline_backtest = run_decision_backtest(feature_df, decisions_by_index=gated_decisions)
    repair_policies_by_horizon: dict[str, list[dict]] = {}
    repair_counts_by_horizon: dict[str, int] = {}
    for horizon in _HORIZONS:
        horizon_key = f"{horizon}d"
        baseline_result = baseline_backtest.get(horizon_key, {})
        selected_rows = list(baseline_result.get("predictions", []))
        repair_candidates = _coverage_repair_candidates(feature_df, gated_decisions, horizon)
        selected_repair_candidates = _select_max_safe_coverage_candidates(selected_rows, repair_candidates)
        repair_policies_by_horizon[horizon_key] = [
            candidate["policy"]
            for candidate in selected_repair_candidates
        ]
        repair_counts_by_horizon[horizon_key] = len(
            {
                _prediction_row_key(row)
                for candidate in selected_repair_candidates
                for row in candidate["rows"]
            }
        )
    _apply_coverage_repair_policies(feature_df, gated_decisions, repair_policies_by_horizon)

    final_backtest = run_decision_backtest(feature_df, decisions_by_index=gated_decisions)
    for horizon_key, result in final_backtest.items():
        raw_result = raw_backtest.get(horizon_key, {})
        result["quality_gate"] = _deployment_quality_gate(result)
        result["direction_quality_gate"] = deepcopy(direction_gates.get(horizon_key, {}))
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
        result["coverage_policy"] = raw_result.get("coverage_policy", _COVERAGE_POLICY_MAX_SAFE)
        result["max_safe_prediction_count"] = result.get("prediction_count")
        result["max_safe_coverage"] = result.get("coverage")
        result["coverage_expansion_signal_count"] = raw_result.get("coverage_expansion_signal_count", 0)
        result["coverage_repair_policy_count"] = len(repair_policies_by_horizon.get(horizon_key, []))
        result["coverage_repair_prediction_count"] = repair_counts_by_horizon.get(horizon_key, 0)
        result["coverage_repair_policies"] = deepcopy(repair_policies_by_horizon.get(horizon_key, []))
    return gated_decisions, final_backtest




__all__ = [name for name in globals() if not name.startswith("__")]
