from __future__ import annotations

from .settings import *
from .features import *

def _model_metadata(
    training_sample_count: int,
    positive_count: int = 0,
    negative_count: int = 0,
    *,
    validation_accuracy: float | None = None,
    validation_precision: float | None = None,
    candidate_count: int = 0,
) -> dict:
    return {
        "type": "blocked_walk_forward_validated_ensemble",
        "positive_label": "continuation",
        "training_scope": "all_available_days",
        "prediction_scope": "latest_day_always_or_bollinger_touch_days",
        "training_sample_count": int(training_sample_count),
        "continuation_training_count": int(positive_count),
        "reversal_training_count": int(negative_count),
        "min_training_samples": _MIN_TRAINING_SAMPLES,
        "validation_accuracy": round(validation_accuracy, 6) if validation_accuracy is not None else None,
        "validation_precision": round(validation_precision, 6) if validation_precision is not None else None,
        "continuation_precision_threshold": _CONTINUATION_PRECISION_THRESHOLD,
        "reversal_precision_threshold": _REVERSAL_PRECISION_THRESHOLD,
        "continuation_deployment_threshold": _CONTINUATION_DEPLOYMENT_THRESHOLD,
        "reversal_deployment_threshold": _REVERSAL_DEPLOYMENT_THRESHOLD,
        "min_continuation_validation_calls": _MIN_CONTINUATION_VALIDATION_CALLS,
        "min_reversal_validation_calls": _MIN_REVERSAL_VALIDATION_CALLS,
        "continuation_hurdle_atr": _DIRECTION_ATR_THRESHOLD,
        "direction_scoring_policy": "close_to_close_trade_sign",
        "flat_price_absolute_tolerance": _FLAT_PRICE_ABSOLUTE_TOLERANCE,
        "flat_reversal_predictions_count_as_correct": True,
        "deployment_min_backtest_calls": _DEPLOY_MIN_BACKTEST_CALLS,
        "deployment_min_backtest_accuracy": _DEPLOY_MIN_BACKTEST_ACCURACY,
        "deployment_min_backtest_reverse_accuracy": _DEPLOY_MIN_BACKTEST_REVERSE_ACCURACY,
        "deployment_min_backtest_continue_accuracy": _DEPLOY_MIN_BACKTEST_CONTINUE_ACCURACY,
        "feature_count": len(_MODEL_FEATURES),
        "candidate_search_count": len(_candidate_specs()),
        "training_history_period": _TRAINING_HISTORY_PERIOD,
        "model_iterations": _MODEL_ITERATIONS,
        "candidate_count": int(candidate_count),
    }


def _no_prediction_horizon(
    horizon: int,
    reason: str,
    *,
    training_sample_count: int = 0,
    positive_count: int = 0,
    negative_count: int = 0,
) -> dict:
    return {
        "status": "no_prediction",
        "predicted_direction": None,
        "continuation_probability": 0.5,
        "reversal_probability": 0.5,
        "continuation_confidence_score": 50,
        "reversal_confidence_score": 50,
        "continuation_validation_precision": None,
        "reversal_validation_precision": None,
        "continuation_validation_count": 0,
        "reversal_validation_count": 0,
        "validation_policy": {
            "validation_count": 0,
            "active_side": None,
            "active_validation_count": 0,
            "continuation": None,
            "reversal": None,
            "selected": None,
        },
        "confidence_score": 0,
        "threshold": _PREDICTION_THRESHOLD,
        "no_prediction_reason": reason,
        "reversal_veto_reason": None,
        "analog_evidence": None,
        "analog_override": False,
        "deployment_quality_gate": None,
        "blocked_prediction": None,
        "contributions": [],
        "model": _model_metadata(training_sample_count, positive_count, negative_count),
    }


def _feature_vector(row: pd.Series) -> np.ndarray:
    values = [_safe_num(row.get(feature), 0.0) for feature in _MODEL_FEATURES]
    vector = np.asarray(values, dtype=float)
    return np.nan_to_num(vector, nan=0.0, posinf=0.0, neginf=0.0)


def _empty_training_matrix() -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    return (
        np.empty((0, len(_MODEL_FEATURES)), dtype=float),
        np.asarray([], dtype=float),
        np.asarray([], dtype=int),
    )


def _feature_side_label(vector: np.ndarray) -> str:
    try:
        touch_side_sign = _safe_num(vector[_MODEL_FEATURES.index("touch_side_sign")], 0.0)
        analysis_side_sign = _safe_num(vector[_MODEL_FEATURES.index("analysis_side_sign")], 0.0)
    except (ValueError, IndexError):
        return "unknown"
    side_sign = touch_side_sign if abs(touch_side_sign) > _EPS else analysis_side_sign
    if side_sign > 0:
        return "upper"
    if side_sign < 0:
        return "lower"
    return "unknown"


def _analog_feature_indices() -> list[int]:
    return [_MODEL_FEATURES.index(feature) for feature in _ANALOG_FEATURES if feature in _MODEL_FEATURES]


def _wilson_lower_bound(success_count: int, sample_count: int, z_value: float = 1.0) -> float:
    if sample_count <= 0:
        return 0.0
    phat = success_count / sample_count
    denominator = 1.0 + (z_value * z_value / sample_count)
    centre = phat + (z_value * z_value / (2.0 * sample_count))
    margin = z_value * math.sqrt((phat * (1.0 - phat) + (z_value * z_value / (4.0 * sample_count))) / sample_count)
    return _clamp((centre - margin) / denominator, 0.0, 1.0)


def _training_side_for_row(row: pd.Series) -> str | None:
    touched_side = row.get("touched_side")
    if touched_side in ("Upper", "Lower"):
        return touched_side

    side_sign = _safe_num(row.get("analysis_side_sign"), 0.0)
    if side_sign > 0:
        return "Upper"
    if side_sign < 0:
        return "Lower"
    return None


def _label_for_index(feature_df: pd.DataFrame, idx: int, horizon: int) -> int | None:
    if idx + horizon >= len(feature_df):
        return None
    row = feature_df.iloc[idx]
    if _safe_bool(row.get("event_risk_blocked")):
        return None
    training_side = _training_side_for_row(row)
    if training_side not in ("Upper", "Lower"):
        return None
    signal_close = _safe_num(row.get("close"), np.nan)
    outcome_close = _safe_num(feature_df.iloc[idx + horizon].get("close"), np.nan)
    if not np.isfinite(signal_close) or not np.isfinite(outcome_close):
        return None
    direction = _actual_direction(training_side, signal_close, outcome_close, _flat_tolerance_for_row(row))
    if direction == "flat":
        return None
    return 1 if direction == "continuation" else 0


def _build_training_matrix(
    feature_df: pd.DataFrame,
    *,
    target_idx: int,
    horizon: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    rows: list[np.ndarray] = []
    labels: list[int] = []
    indices: list[int] = []

    for idx in range(target_idx):
        # All historical bars can train the model once their horizon outcome is known.
        if idx + horizon > target_idx:
            continue
        label = _label_for_index(feature_df, idx, horizon)
        if label is None:
            continue
        rows.append(_feature_vector(feature_df.iloc[idx]))
        labels.append(label)
        indices.append(idx)

    if not rows:
        return _empty_training_matrix()
    return np.vstack(rows), np.asarray(labels, dtype=float), np.asarray(indices, dtype=int)


def _fit_logistic_arrays(x_train: np.ndarray, y_train: np.ndarray, *, kind: str, lookback: int | None) -> dict | None:
    sample_count = int(len(y_train))
    if sample_count < _MIN_TRAINING_SAMPLES:
        return None
    positive_count = int(np.sum(y_train == 1.0))
    negative_count = int(np.sum(y_train == 0.0))
    if positive_count == 0 or negative_count == 0:
        return None

    mean = x_train.mean(axis=0)
    scale = x_train.std(axis=0)
    scale = np.where(scale < _EPS, 1.0, scale)
    x_scaled = np.clip((x_train - mean) / scale, -_MODEL_FEATURE_CLIP, _MODEL_FEATURE_CLIP)

    pos_rate = _clamp(float(y_train.mean()), 0.02, 0.98)
    intercept = math.log(pos_rate / (1.0 - pos_rate))
    coef = np.zeros(x_scaled.shape[1], dtype=float)
    recency_weights = np.exp(np.linspace(-1.6, 0.0, sample_count))
    recency_weights = recency_weights / recency_weights.mean()

    for _ in range(_MODEL_ITERATIONS):
        logits = np.clip(x_scaled @ coef + intercept, -30.0, 30.0)
        probs = 1.0 / (1.0 + np.exp(-logits))
        error = (probs - y_train) * recency_weights
        grad_coef = (x_scaled.T @ error) / sample_count + (_MODEL_L2 * coef)
        grad_intercept = float(error.mean())
        coef -= _MODEL_LEARNING_RATE * grad_coef
        intercept -= _MODEL_LEARNING_RATE * grad_intercept

    return {
        "kind": kind,
        "lookback": lookback,
        "sample_count": sample_count,
        "positive_count": positive_count,
        "negative_count": negative_count,
        "mean": mean,
        "scale": scale,
        "coef": coef,
        "intercept": float(intercept),
    }


def _fit_rbf_arrays(
    x_train: np.ndarray,
    y_train: np.ndarray,
    *,
    kind: str,
    lookback: int | None,
    gamma: float,
) -> dict | None:
    sample_count = int(len(y_train))
    if sample_count < _MIN_TRAINING_SAMPLES:
        return None
    positive_count = int(np.sum(y_train == 1.0))
    negative_count = int(np.sum(y_train == 0.0))
    if positive_count == 0 or negative_count == 0:
        return None

    mean = x_train.mean(axis=0)
    scale = x_train.std(axis=0)
    scale = np.where(scale < _EPS, 1.0, scale)
    x_scaled = np.clip((x_train - mean) / scale, -_MODEL_FEATURE_CLIP, _MODEL_FEATURE_CLIP)
    recency_weights = np.exp(np.linspace(-1.8, 0.0, sample_count))
    pos_centroid = x_scaled[y_train == 1.0].mean(axis=0)
    neg_centroid = x_scaled[y_train == 0.0].mean(axis=0)

    return {
        "kind": kind,
        "lookback": lookback,
        "gamma": gamma,
        "sample_count": sample_count,
        "positive_count": positive_count,
        "negative_count": negative_count,
        "mean": mean,
        "scale": scale,
        "x_scaled": x_scaled,
        "y_train": y_train,
        "recency_weights": recency_weights,
        "prior": float(y_train.mean()),
        "pos_centroid": pos_centroid,
        "neg_centroid": neg_centroid,
    }


def _slice_training_records(
    x_train: np.ndarray,
    y_train: np.ndarray,
    indices: np.ndarray,
    lookback: int | None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    if lookback is not None and len(y_train) > lookback:
        return x_train[-lookback:], y_train[-lookback:], indices[-lookback:]
    return x_train, y_train, indices


def _fit_candidate(
    x_train: np.ndarray,
    y_train: np.ndarray,
    indices: np.ndarray,
    spec: dict,
) -> dict | None:
    x_slice, y_slice, idx_slice = _slice_training_records(x_train, y_train, indices, spec.get("lookback"))
    if spec["kind"] == "logistic":
        model = _fit_logistic_arrays(x_slice, y_slice, kind="logistic", lookback=spec.get("lookback"))
    else:
        model = _fit_rbf_arrays(
            x_slice,
            y_slice,
            kind="rbf",
            lookback=spec.get("lookback"),
            gamma=spec["gamma"],
        )
    if model is not None:
        model["indices"] = idx_slice
    return model


def _predict_candidate_probability(x_raw: np.ndarray, model: dict) -> float:
    x_scaled = np.clip((x_raw - model["mean"]) / model["scale"], -_MODEL_FEATURE_CLIP, _MODEL_FEATURE_CLIP)
    if model["kind"] == "logistic":
        return _sigmoid(float(x_scaled @ model["coef"] + model["intercept"]))

    diff = model["x_scaled"] - x_scaled
    distance = np.mean(diff * diff, axis=1)
    similarity = np.exp(-distance / (2.0 * model["gamma"] * model["gamma"]))
    weights = similarity * model["recency_weights"]
    weight_sum = float(weights.sum())
    if weight_sum <= _EPS:
        return _clamp(float(model["prior"]), 0.01, 0.99)
    return _clamp(float(np.dot(weights, model["y_train"]) / weight_sum), 0.01, 0.99)


def _candidate_specs() -> list[dict]:
    specs: list[dict] = [{"kind": "logistic", "lookback": lookback} for lookback in _LOGISTIC_LOOKBACKS]
    for lookback, gamma in _RBF_SPECS:
        specs.append({"kind": "rbf", "lookback": lookback, "gamma": gamma})
    return specs


def _validate_candidate(
    x_train: np.ndarray,
    y_train: np.ndarray,
    indices: np.ndarray,
    *,
    horizon: int,
    spec: dict,
) -> dict | None:
    if len(y_train) < _MIN_TRAINING_SAMPLES + _MIN_VALIDATION_SAMPLES:
        return None

    val_count = min(
        max(_MIN_VALIDATION_SAMPLES, int(round(len(y_train) * 0.35))),
        max(_MIN_VALIDATION_SAMPLES, len(y_train) // 2),
        _MAX_VALIDATION_SAMPLES,
    )
    start = max(_MIN_TRAINING_SAMPLES, len(y_train) - val_count)
    train_positions = np.asarray([], dtype=int)
    while start <= len(y_train) - _MIN_VALIDATION_SAMPLES:
        first_validation_idx = int(indices[start])
        train_positions = np.where(indices[:start] + horizon <= first_validation_idx)[0]
        if len(train_positions) >= _MIN_TRAINING_SAMPLES:
            break
        start += 1

    if len(train_positions) < _MIN_TRAINING_SAMPLES or start > len(y_train) - _MIN_VALIDATION_SAMPLES:
        return None

    candidate = _fit_candidate(
        x_train[train_positions],
        y_train[train_positions],
        indices[train_positions],
        spec,
    )
    if candidate is None:
        return None

    predictions: list[dict] = []
    for pos in range(start, len(y_train)):
        probability = _predict_candidate_probability(x_train[pos], candidate)
        predicted = 1 if probability >= 0.5 else 0
        predictions.append(
            {
                "index": int(indices[pos]),
                "predicted": predicted,
                "actual": int(y_train[pos]),
                "probability": float(probability),
                "side": _feature_side_label(x_train[pos]),
            }
        )

    if len(predictions) < _MIN_VALIDATION_SAMPLES:
        return None

    correct = sum(1 for item in predictions if item["predicted"] == item["actual"])
    brier = float(np.mean([(item["probability"] - item["actual"]) ** 2 for item in predictions]))
    continuation_preds = [item for item in predictions if item["predicted"] == 1]
    reversal_preds = [item for item in predictions if item["predicted"] == 0]
    continuation_precision = (
        sum(1 for item in continuation_preds if item["actual"] == 1) / len(continuation_preds)
        if continuation_preds
        else None
    )
    reversal_precision = (
        sum(1 for item in reversal_preds if item["actual"] == 0) / len(reversal_preds)
        if reversal_preds
        else None
    )

    return {
        "validation_count": len(predictions),
        "accuracy": correct / len(predictions),
        "brier": brier,
        "continuation_precision": continuation_precision,
        "reversal_precision": reversal_precision,
        "continuation_prediction_count": len(continuation_preds),
        "reversal_prediction_count": len(reversal_preds),
        "predictions": predictions,
    }


def _combine_validation_predictions(candidates: list[dict]) -> list[dict]:
    combined: dict[int, dict] = {}
    for candidate in candidates:
        weight = _safe_num(candidate.get("weight"), 0.0)
        if weight <= 0:
            continue
        validation = candidate.get("validation", {})
        for item in validation.get("predictions", []):
            idx = int(item.get("index", -1))
            if idx < 0:
                continue
            actual = int(item.get("actual", 0))
            probability = _clamp(_safe_num(item.get("probability"), 0.5), 0.01, 0.99)
            bucket = combined.setdefault(
                idx,
                {
                    "weighted_probability": 0.0,
                    "weight": 0.0,
                    "actual": actual,
                    "side": item.get("side", "unknown"),
                },
            )
            bucket["weighted_probability"] += weight * probability
            bucket["weight"] += weight
            bucket["actual"] = actual
            if bucket.get("side") == "unknown" and item.get("side") in ("upper", "lower"):
                bucket["side"] = item.get("side")

    rows: list[dict] = []
    for idx, item in sorted(combined.items()):
        weight = _safe_num(item.get("weight"), 0.0)
        if weight <= _EPS:
            continue
        rows.append(
            {
                "index": idx,
                "probability": _clamp(item["weighted_probability"] / weight, 0.01, 0.99),
                "actual": int(item.get("actual", 0)),
                "side": item.get("side", "unknown"),
            }
        )
    return rows


def _direction_policy_from_rows(
    rows: list[dict],
    *,
    direction: str,
    precision_target: float,
    min_calls: int,
) -> dict | None:
    if direction not in ("continuation", "reversal"):
        return None
    if not rows:
        return None

    effective_min_calls = min(min_calls, max(3, len(rows) // 2))
    probabilities = sorted({_clamp(_safe_num(row.get("probability"), 0.5), 0.01, 0.99) for row in rows})
    best: dict | None = None

    if direction == "continuation":
        for cutoff in sorted(probabilities, reverse=True):
            selected = [row for row in rows if _safe_num(row.get("probability"), 0.5) >= cutoff]
            call_count = len(selected)
            if call_count < effective_min_calls:
                continue
            correct_count = sum(1 for row in selected if int(row.get("actual", 0)) == 1)
            precision = correct_count / call_count
            if precision < precision_target:
                continue
            # Descending cutoffs means later passing cutoffs are more permissive.
            best = {
                "direction": direction,
                "cutoff": float(cutoff),
                "precision": float(precision),
                "call_count": int(call_count),
                "correct_count": int(correct_count),
                "min_call_count": int(effective_min_calls),
            }
    else:
        for cutoff in probabilities:
            selected = [row for row in rows if _safe_num(row.get("probability"), 0.5) <= cutoff]
            call_count = len(selected)
            if call_count < effective_min_calls:
                continue
            correct_count = sum(1 for row in selected if int(row.get("actual", 1)) == 0)
            precision = correct_count / call_count
            if precision < precision_target:
                continue
            # Ascending cutoffs means later passing cutoffs are more permissive.
            best = {
                "direction": direction,
                "cutoff": float(cutoff),
                "precision": float(precision),
                "call_count": int(call_count),
                "correct_count": int(correct_count),
                "min_call_count": int(effective_min_calls),
            }

    return best


def _derive_precision_policy(candidates: list[dict]) -> dict:
    rows = _combine_validation_predictions(candidates)
    groups = {
        "overall": rows,
        "upper": [row for row in rows if row.get("side") == "upper"],
        "lower": [row for row in rows if row.get("side") == "lower"],
    }
    policy: dict[str, Any] = {"validation_count": len(rows)}
    for group_name, group_rows in groups.items():
        policy[group_name] = {
            "validation_count": len(group_rows),
            "continuation": _direction_policy_from_rows(
                group_rows,
                direction="continuation",
                precision_target=_CONTINUATION_PRECISION_THRESHOLD,
                min_calls=_MIN_CONTINUATION_VALIDATION_CALLS,
            ),
            "reversal": _direction_policy_from_rows(
                group_rows,
                direction="reversal",
                precision_target=_REVERSAL_PRECISION_THRESHOLD,
                min_calls=_MIN_REVERSAL_VALIDATION_CALLS,
            ),
        }
    policy["continuation"] = policy["overall"]["continuation"]
    policy["reversal"] = policy["overall"]["reversal"]
    return policy


def _analog_meta_label(row: pd.Series, model: dict, direction: str) -> dict:
    x_train = model.get("x_train")
    y_train = model.get("y_train")
    if x_train is None or y_train is None:
        return {"status": "missing", "reason": "missing_analog_training_data"}
    if len(y_train) < min(_ANALOG_K_VALUES):
        return {"status": "missing", "reason": "insufficient_analog_training_data"}

    x_train = np.asarray(x_train, dtype=float)
    y_train = np.asarray(y_train, dtype=float)
    x_raw = _feature_vector(row)
    feature_indices = _analog_feature_indices()
    if not feature_indices:
        return {"status": "missing", "reason": "missing_analog_features"}

    target_side = _feature_side_label(x_raw)
    side_labels = np.asarray([_feature_side_label(vector) for vector in x_train])
    same_side_mask = side_labels == target_side
    side_count = int(np.sum(same_side_mask))
    if side_count >= min(_ANALOG_K_VALUES):
        eligible_mask = same_side_mask
        side_scope = target_side
    else:
        eligible_mask = np.ones(len(y_train), dtype=bool)
        side_scope = "all"

    x_pool = x_train[eligible_mask][:, feature_indices]
    y_pool = y_train[eligible_mask]
    if len(y_pool) < min(_ANALOG_K_VALUES):
        return {
            "status": "missing",
            "reason": "insufficient_analog_pool",
            "side_scope": side_scope,
            "same_side_count": side_count,
            "pool_count": int(len(y_pool)),
        }

    target = x_raw[feature_indices]
    median = np.nanmedian(x_pool, axis=0)
    q75 = np.nanpercentile(x_pool, 75, axis=0)
    q25 = np.nanpercentile(x_pool, 25, axis=0)
    scale = np.where((q75 - q25) < _EPS, np.nanstd(x_pool, axis=0), q75 - q25)
    scale = np.where(scale < _EPS, 1.0, scale)

    x_scaled = np.clip((np.nan_to_num(x_pool) - median) / scale, -_MODEL_FEATURE_CLIP, _MODEL_FEATURE_CLIP)
    target_scaled = np.clip((np.nan_to_num(target) - median) / scale, -_MODEL_FEATURE_CLIP, _MODEL_FEATURE_CLIP)
    distance = np.sqrt(np.mean((x_scaled - target_scaled) ** 2, axis=1))
    order = np.argsort(distance)
    desired_label = 1.0 if direction == "continuation" else 0.0

    best: dict | None = None
    for k_value in _ANALOG_K_VALUES:
        k = min(k_value, len(order))
        if k < min(_ANALOG_K_VALUES):
            continue
        nearest = order[:k]
        success_count = int(np.sum(y_pool[nearest] == desired_label))
        precision = success_count / k
        posterior = (success_count + 1.0) / (k + 2.0)
        lower_bound = _wilson_lower_bound(success_count, k)
        avg_distance = float(np.mean(distance[nearest]))
        candidate = {
            "status": "ready",
            "direction": direction,
            "side_scope": side_scope,
            "same_side_count": side_count,
            "pool_count": int(len(y_pool)),
            "neighbor_count": int(k),
            "success_count": success_count,
            "precision": float(precision),
            "posterior_probability": float(posterior),
            "lower_bound": float(lower_bound),
            "avg_distance": avg_distance,
        }
        if best is None or (
            candidate["posterior_probability"],
            candidate["precision"],
            -candidate["avg_distance"],
        ) > (
            best["posterior_probability"],
            best["precision"],
            -best["avg_distance"],
        ):
            best = candidate

    return best or {"status": "missing", "reason": "insufficient_analog_neighbors"}


def _fit_supervised_model(
    feature_df: pd.DataFrame,
    *,
    target_idx: int,
    horizon: int,
) -> tuple[dict | None, str | None]:
    x_train, y_train, indices = _build_training_matrix(feature_df, target_idx=target_idx, horizon=horizon)
    sample_count = int(len(y_train))
    positive_count = int(np.sum(y_train == 1.0))
    negative_count = int(np.sum(y_train == 0.0))

    if sample_count < _MIN_TRAINING_SAMPLES:
        return {
            "sample_count": sample_count,
            "positive_count": positive_count,
            "negative_count": negative_count,
        }, "insufficient_training_data"
    if positive_count == 0 or negative_count == 0:
        return {
            "sample_count": sample_count,
            "positive_count": positive_count,
            "negative_count": negative_count,
        }, "insufficient_class_balance"

    candidates: list[dict] = []
    for spec in _candidate_specs():
        validation = _validate_candidate(x_train, y_train, indices, horizon=horizon, spec=spec)
        if validation is None:
            continue
        validation_rows = validation.get("predictions", [])
        continuation_zone = _direction_policy_from_rows(
            validation_rows,
            direction="continuation",
            precision_target=_CONTINUATION_PRECISION_THRESHOLD,
            min_calls=_MIN_CONTINUATION_VALIDATION_CALLS,
        )
        reversal_zone = _direction_policy_from_rows(
            validation_rows,
            direction="reversal",
            precision_target=_REVERSAL_PRECISION_THRESHOLD,
            min_calls=_MIN_REVERSAL_VALIDATION_CALLS,
        )
        policy_edge = max(
            (
                _safe_num(continuation_zone.get("precision"), 0.0) - _CONTINUATION_PRECISION_THRESHOLD
                if continuation_zone
                else 0.0
            ),
            (
                _safe_num(reversal_zone.get("precision"), 0.0) - _REVERSAL_PRECISION_THRESHOLD
                if reversal_zone
                else 0.0
            ),
        )
        if validation["accuracy"] < _MIN_VALIDATION_ACCURACY and not (continuation_zone or reversal_zone):
            continue
        candidate = _fit_candidate(x_train, y_train, indices, spec)
        if candidate is None:
            continue
        edge = max(0.001, validation["accuracy"] - 0.5, policy_edge)
        candidate["validation"] = validation
        candidate["weight"] = edge * math.sqrt(validation["validation_count"]) / max(validation["brier"], 0.05)
        candidates.append(candidate)

    if not candidates:
        return {
            "sample_count": sample_count,
            "positive_count": positive_count,
            "negative_count": negative_count,
        }, "weak_validation_edge"

    return {
        "kind": "ensemble",
        "sample_count": sample_count,
        "positive_count": positive_count,
        "negative_count": negative_count,
        "candidates": candidates,
        "precision_policy": _derive_precision_policy(candidates),
        "x_train": x_train,
        "y_train": y_train,
        "indices": indices,
    }, None


def _predict_with_model(row: pd.Series, horizon: int, model: dict) -> dict:
    x_raw = _feature_vector(row)
    candidates = model.get("candidates", [])
    weighted_probability = 0.0
    total_weight = 0.0
    for candidate in candidates:
        weight = _safe_num(candidate.get("weight"), 0.0)
        if weight <= 0:
            continue
        weighted_probability += weight * _predict_candidate_probability(x_raw, candidate)
        total_weight += weight

    raw_probability = _clamp(weighted_probability / total_weight if total_weight > _EPS else 0.5, 0.01, 0.99)
    raw_predicted_direction = "continuation" if raw_probability >= 0.5 else "reversal"
    precision_policy = model.get("precision_policy", {}) or {}
    row_side = _training_side_for_row(row)
    side_policy_key = "upper" if row_side == "Upper" else "lower" if row_side == "Lower" else "overall"
    side_policy = precision_policy.get(side_policy_key) if isinstance(precision_policy.get(side_policy_key), dict) else None
    overall_policy = precision_policy.get("overall") if isinstance(precision_policy.get("overall"), dict) else precision_policy
    active_policy = side_policy or overall_policy or {}
    continuation_policy = active_policy.get("continuation")
    reversal_policy = active_policy.get("reversal")
    validation_accuracy = (
        sum(_safe_num(c.get("weight")) * _safe_num(c.get("validation", {}).get("accuracy")) for c in candidates)
        / total_weight
        if total_weight > _EPS
        else None
    )
    continuation_precision_values = [
        (_safe_num(c.get("weight")), c.get("validation", {}).get("continuation_precision"))
        for c in candidates
        if c.get("validation", {}).get("continuation_precision") is not None
    ]
    reversal_precision_values = [
        (_safe_num(c.get("weight")), c.get("validation", {}).get("reversal_precision"))
        for c in candidates
        if c.get("validation", {}).get("reversal_precision") is not None
    ]
    continuation_validation_precision = (
        sum(weight * _safe_num(value) for weight, value in continuation_precision_values)
        / sum(weight for weight, _ in continuation_precision_values)
        if continuation_precision_values and sum(weight for weight, _ in continuation_precision_values) > _EPS
        else None
    )
    reversal_validation_precision = (
        sum(weight * _safe_num(value) for weight, value in reversal_precision_values)
        / sum(weight for weight, _ in reversal_precision_values)
        if reversal_precision_values and sum(weight for weight, _ in reversal_precision_values) > _EPS
        else None
    )
    continuation_validation_count_values = [
        (_safe_num(c.get("weight")), _safe_num(c.get("validation", {}).get("continuation_prediction_count")))
        for c in candidates
    ]
    reversal_validation_count_values = [
        (_safe_num(c.get("weight")), _safe_num(c.get("validation", {}).get("reversal_prediction_count")))
        for c in candidates
    ]
    continuation_validation_count = (
        sum(weight * value for weight, value in continuation_validation_count_values)
        / sum(weight for weight, _ in continuation_validation_count_values)
        if continuation_validation_count_values
        and sum(weight for weight, _ in continuation_validation_count_values) > _EPS
        else 0.0
    )
    reversal_validation_count = (
        sum(weight * value for weight, value in reversal_validation_count_values)
        / sum(weight for weight, _ in reversal_validation_count_values)
        if reversal_validation_count_values
        and sum(weight for weight, _ in reversal_validation_count_values) > _EPS
        else 0.0
    )

    policy_matches: list[tuple[str, dict]] = []
    if isinstance(continuation_policy, dict) and raw_probability >= _safe_num(continuation_policy.get("cutoff"), 1.0):
        policy_matches.append(("continuation", continuation_policy))
    if isinstance(reversal_policy, dict) and raw_probability <= _safe_num(reversal_policy.get("cutoff"), 0.0):
        policy_matches.append(("reversal", reversal_policy))

    selected_direction: str | None = None
    selected_policy: dict | None = None
    if policy_matches:
        selected_direction, selected_policy = sorted(
            policy_matches,
            key=lambda item: (
                _safe_num(item[1].get("precision"), 0.0),
                _safe_num(item[1].get("call_count"), 0.0),
            ),
            reverse=True,
        )[0]

    if isinstance(continuation_policy, dict):
        continuation_validation_precision = _safe_num(
            continuation_policy.get("precision"),
            _safe_num(continuation_validation_precision, 0.0),
        )
        continuation_validation_count = _safe_num(
            continuation_policy.get("call_count"),
            continuation_validation_count,
        )
    if isinstance(reversal_policy, dict):
        reversal_validation_precision = _safe_num(
            reversal_policy.get("precision"),
            _safe_num(reversal_validation_precision, 0.0),
        )
        reversal_validation_count = _safe_num(
            reversal_policy.get("call_count"),
            reversal_validation_count,
        )

    balance = min(model["positive_count"], model["negative_count"]) / max(model["positive_count"], model["negative_count"])
    sample_strength = _clamp((model["sample_count"] - _MIN_TRAINING_SAMPLES) / 16.0, 0.0, 1.0)
    validation_strength = _clamp(((_safe_num(validation_accuracy, 0.5) - 0.45) / 0.30), 0.0, 1.0)
    policy_strength = _safe_num(selected_policy.get("precision"), 0.5) if selected_policy else 0.5
    shrink = balance * sample_strength * validation_strength * _clamp((policy_strength - 0.5) / 0.35, 0.15, 1.0)
    continuation_probability = raw_probability
    reversal_probability = 1.0 - continuation_probability

    if selected_direction == "continuation" and selected_policy is not None:
        continuation_probability = _clamp(
            max(continuation_probability, _safe_num(selected_policy.get("precision"), continuation_probability)),
            0.01,
            0.99,
        )
        reversal_probability = 1.0 - continuation_probability
    elif selected_direction == "reversal" and selected_policy is not None:
        reversal_probability = _clamp(
            max(reversal_probability, _safe_num(selected_policy.get("precision"), reversal_probability)),
            0.01,
            0.99,
        )
        continuation_probability = 1.0 - reversal_probability

    max_probability = max(continuation_probability, reversal_probability)
    confidence_score = int(round(max_probability * 100.0))
    validation_precision = _safe_num(selected_policy.get("precision"), np.nan) if selected_policy else np.nan
    if not np.isfinite(validation_precision):
        precision_key = (
            "continuation_precision"
            if raw_predicted_direction == "continuation"
            else "reversal_precision"
        )
        validation_precision_values = [
            (_safe_num(c.get("weight")), c.get("validation", {}).get(precision_key))
            for c in candidates
            if c.get("validation", {}).get(precision_key) is not None
        ]
        validation_precision = (
            sum(weight * _safe_num(value) for weight, value in validation_precision_values)
            / sum(weight for weight, _ in validation_precision_values)
            if validation_precision_values and sum(weight for weight, _ in validation_precision_values) > _EPS
            else np.nan
        )

    contribution_by_feature = {feature: 0.0 for feature in _MODEL_FEATURES}
    for candidate in candidates:
        model_weight = _safe_num(candidate.get("weight"), 0.0) / total_weight if total_weight > _EPS else 0.0
        x_scaled = np.clip((x_raw - candidate["mean"]) / candidate["scale"], -_MODEL_FEATURE_CLIP, _MODEL_FEATURE_CLIP)
        if candidate["kind"] == "logistic":
            signal = candidate["coef"] * x_scaled
        else:
            signal = (candidate["pos_centroid"] - candidate["neg_centroid"]) * x_scaled * 0.25
        for feature, contribution in zip(_MODEL_FEATURES, signal):
            contribution_by_feature[feature] += float(model_weight * contribution * shrink)

    contributions = [
        {
            "horizon": f"{horizon}d",
            "feature": feature,
            "value": _value_for_payload(value),
            "impact": "continuation" if contribution >= 0 else "reversal",
            "contribution": round(contribution, 6),
        }
        for feature, value, contribution in zip(_MODEL_FEATURES, x_raw, [contribution_by_feature[f] for f in _MODEL_FEATURES])
    ]

    raw_confidence = max(raw_probability, 1.0 - raw_probability)
    decision_direction = selected_direction
    analog_override = False
    if decision_direction is None and raw_confidence >= _PREDICTION_THRESHOLD:
        decision_direction = raw_predicted_direction
        analog_override = True

    reversal_veto_reason = _reversal_veto_reason(row, horizon) if decision_direction == "reversal" else None
    analog_evidence = _analog_meta_label(row, model, decision_direction) if decision_direction else None
    analog_reason: str | None = None
    if decision_direction == "reversal" and analog_evidence is not None:
        if analog_evidence.get("status") != "ready":
            analog_reason = analog_evidence.get("reason") or "insufficient_analog_evidence"
        elif (
            _safe_num(analog_evidence.get("precision"), 0.0) < _ANALOG_MIN_REVERSE_PRECISION
            or _safe_num(analog_evidence.get("posterior_probability"), 0.0) < _ANALOG_MIN_REVERSE_POSTERIOR
        ):
            analog_reason = "weak_reverse_analog_evidence"
    elif decision_direction == "continuation" and analog_evidence is not None:
        if analog_evidence.get("status") != "ready":
            analog_reason = analog_evidence.get("reason") or "insufficient_analog_evidence"
        elif (
            _safe_num(analog_evidence.get("precision"), 0.0) < _ANALOG_MIN_CONTINUE_PRECISION
            or _safe_num(analog_evidence.get("posterior_probability"), 0.0) < _ANALOG_MIN_CONTINUE_POSTERIOR
        ):
            analog_reason = "weak_continue_analog_evidence"

    if analog_evidence and analog_evidence.get("status") == "ready":
        analog_probability = _safe_num(analog_evidence.get("posterior_probability"), max_probability)
        if decision_direction == "continuation":
            continuation_probability = _clamp(
                max(continuation_probability, analog_probability)
                if analog_override
                else min(continuation_probability, analog_probability),
                0.01,
                0.99,
            )
            reversal_probability = 1.0 - continuation_probability
        elif decision_direction == "reversal":
            reversal_probability = _clamp(
                max(reversal_probability, analog_probability)
                if analog_override
                else min(reversal_probability, analog_probability),
                0.01,
                0.99,
            )
            continuation_probability = 1.0 - reversal_probability
        max_probability = max(continuation_probability, reversal_probability)
        confidence_score = int(round(max_probability * 100.0))

    if decision_direction is None:
        status = "no_prediction"
        predicted_direction = None
        no_prediction_reason = "low_confidence" if raw_confidence < _PREDICTION_THRESHOLD else "no_validated_precision_zone"
    elif reversal_veto_reason is not None:
        status = "no_prediction"
        predicted_direction = None
        no_prediction_reason = reversal_veto_reason
    elif analog_reason is not None:
        status = "no_prediction"
        predicted_direction = None
        no_prediction_reason = analog_reason
    elif decision_direction == "reversal" and reversal_probability < _REVERSAL_DEPLOYMENT_THRESHOLD:
        status = "no_prediction"
        predicted_direction = None
        no_prediction_reason = "weak_reverse_deployment_edge"
    elif decision_direction == "continuation" and continuation_probability < _CONTINUATION_DEPLOYMENT_THRESHOLD:
        status = "no_prediction"
        predicted_direction = None
        no_prediction_reason = "weak_continue_deployment_edge"
    elif max_probability < _PREDICTION_THRESHOLD:
        status = "no_prediction"
        predicted_direction = None
        no_prediction_reason = "low_confidence"
    else:
        status = "prediction"
        predicted_direction = decision_direction
        no_prediction_reason = None

    return {
        "status": status,
        "predicted_direction": predicted_direction,
        "continuation_probability": round(continuation_probability, 6),
        "reversal_probability": round(reversal_probability, 6),
        "continuation_confidence_score": int(round(continuation_probability * 100.0)),
        "reversal_confidence_score": int(round(reversal_probability * 100.0)),
        "continuation_validation_precision": (
            round(continuation_validation_precision, 6)
            if continuation_validation_precision is not None
            else None
        ),
        "reversal_validation_precision": (
            round(reversal_validation_precision, 6)
            if reversal_validation_precision is not None
            else None
        ),
        "continuation_validation_count": round(continuation_validation_count, 3),
        "reversal_validation_count": round(reversal_validation_count, 3),
        "validation_policy": {
            "validation_count": int(_safe_num(precision_policy.get("validation_count"), 0)),
            "active_side": side_policy_key,
            "active_validation_count": int(_safe_num(active_policy.get("validation_count"), 0)),
            "continuation": deepcopy(continuation_policy) if isinstance(continuation_policy, dict) else None,
            "reversal": deepcopy(reversal_policy) if isinstance(reversal_policy, dict) else None,
            "selected": deepcopy(selected_policy) if selected_policy is not None else None,
        },
        "confidence_score": confidence_score,
        "threshold": _PREDICTION_THRESHOLD,
        "no_prediction_reason": no_prediction_reason,
        "reversal_veto_reason": reversal_veto_reason,
        "analog_evidence": deepcopy(analog_evidence) if analog_evidence is not None else None,
        "analog_override": analog_override,
        "deployment_quality_gate": None,
        "blocked_prediction": None,
        "contributions": sorted(
            contributions,
            key=lambda item: abs(_safe_num(item.get("contribution"))),
            reverse=True,
        )[:10],
        "model": _model_metadata(
            model["sample_count"],
            model["positive_count"],
            model["negative_count"],
            validation_accuracy=validation_accuracy,
            validation_precision=float(validation_precision) if np.isfinite(validation_precision) else None,
            candidate_count=len(candidates),
        ),
    }




__all__ = [name for name in globals() if not name.startswith("__")]
