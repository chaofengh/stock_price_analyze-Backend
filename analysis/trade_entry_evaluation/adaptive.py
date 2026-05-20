from __future__ import annotations

from copy import deepcopy
import math
import numpy as np
import pandas as pd
from . import features, model as model_utils, settings
from .playbooks import _actual_direction_for_index

def _adaptive_profile_key(profile: dict) -> tuple:
    return (
        profile["id"],
        int(profile["top_features"]),
        int(profile["neighbors"]),
        bool(profile["same_side"]),
        float(profile["reversal_confidence"]),
        float(profile["continuation_confidence"]),
        tuple(profile.get("allowed_directions") or ()),
    )


def _adaptive_profiles_for_horizon(horizon: int) -> list[dict]:
    if horizon <= 5:
        core_ids = ("compact_6x20", "strict_6x10", "same_side_8x10")
        expansion_ids = ("strict_same_side_4x10", "same_side_8x8", "balanced_16x16", "same_side_12x12")
        opportunity_ids = ("trend_8x24", "trend_12x24", "same_side_trend_6x16")
    else:
        core_ids = ("same_side_8x8", "strict_same_side_4x10", "balanced_16x16")
        expansion_ids = ("broad_24x16", "strict_24x20", "same_side_12x12", "broad_24x12")
        opportunity_ids = ("trend_8x24", "trend_12x24", "same_side_trend_6x16", "broad_consensus_32x24")

    profiles: list[dict] = []
    for profile in settings._ADAPTIVE_ANALOG_PROFILES:
        if profile["id"] in core_ids:
            item = deepcopy(profile)
            item["tier"] = "core"
            profiles.append(item)
        elif profile["id"] in expansion_ids:
            item = deepcopy(profile)
            item["tier"] = "expansion"
            profiles.append(item)
        elif profile["id"] in opportunity_ids:
            item = deepcopy(profile)
            item["tier"] = "opportunity"
            profiles.append(item)
    return profiles


def _adaptive_feature_columns(feature_df: pd.DataFrame) -> list[str]:
    return [
        feature
        for feature in settings._MODEL_FEATURES
        if feature in feature_df.columns and feature != "event_risk_blocked"
    ]


def _adaptive_state(feature_df: pd.DataFrame, horizon: int) -> dict:
    cache = feature_df.attrs.get("_adaptive_analog_state")
    if not isinstance(cache, settings._NoDeepcopyDict):
        cache = settings._NoDeepcopyDict(cache or {})
        feature_df.attrs["_adaptive_analog_state"] = cache
    if horizon in cache:
        return cache[horizon]

    feature_columns = _adaptive_feature_columns(feature_df)
    if feature_columns:
        x_values = feature_df[feature_columns].apply(pd.to_numeric, errors="coerce").to_numpy(dtype=float)
        x_values = np.nan_to_num(x_values, nan=0.0, posinf=0.0, neginf=0.0)
    else:
        x_values = np.empty((len(feature_df), 0), dtype=float)

    labels = np.full(len(feature_df), np.nan, dtype=float)
    for idx in range(len(feature_df)):
        label = model_utils._label_for_index(feature_df, idx, horizon)
        if label is not None:
            labels[idx] = float(label)

    side_sign = (
        pd.to_numeric(feature_df.get("analysis_side_sign", pd.Series(0.0, index=feature_df.index)), errors="coerce")
        .fillna(0.0)
        .to_numpy(dtype=float)
    )
    touch_weight = np.asarray(
        [1.0 if side in ("Upper", "Lower") else 0.0 for side in feature_df.get("touched_side", [])],
        dtype=float,
    )

    state = {
        "feature_columns": feature_columns,
        "x": x_values,
        "labels": labels,
        "valid_labels": np.isfinite(labels),
        "side_sign": side_sign,
        "touch_weight": touch_weight,
        "row_context_cache": {},
        "raw_cache": {},
    }
    cache[horizon] = state
    return state


def _adaptive_training_indices(state: dict, target_idx: int, horizon: int) -> np.ndarray:
    known_limit = target_idx - horizon
    if known_limit < 0:
        return np.asarray([], dtype=int)
    all_indices = np.arange(len(state["labels"]), dtype=int)
    return all_indices[state["valid_labels"] & (all_indices <= known_limit)]


def _adaptive_row_context(feature_df: pd.DataFrame, row_index: int, horizon: int) -> dict | None:
    state = _adaptive_state(feature_df, horizon)
    if row_index in state["row_context_cache"]:
        return state["row_context_cache"][row_index]

    feature_columns = state["feature_columns"]
    if not feature_columns:
        state["row_context_cache"][row_index] = None
        return None

    train_idx = _adaptive_training_indices(state, row_index, horizon)
    if len(train_idx) < settings._ADAPTIVE_MIN_TRAINING_ROWS:
        state["row_context_cache"][row_index] = None
        return None

    y_train = state["labels"][train_idx].astype(int)
    if len(set(y_train.tolist())) < 2:
        state["row_context_cache"][row_index] = None
        return None

    x_train = state["x"][train_idx]
    median = np.median(x_train, axis=0)
    mad = np.median(np.abs(x_train - median), axis=0)
    mad = np.where(mad < 1e-6, 1.0, mad)
    z_train = np.clip((x_train - median) / mad, -6.0, 6.0)
    z_target = np.clip((state["x"][row_index] - median) / mad, -6.0, 6.0)

    positive_median = np.median(z_train[y_train == 1], axis=0)
    negative_median = np.median(z_train[y_train == 0], axis=0)
    feature_strength = np.nan_to_num(np.abs(positive_median - negative_median), nan=0.0)
    context = {
        "train_idx": train_idx,
        "y_train": y_train,
        "z_train": z_train,
        "z_target": z_target,
        "feature_strength": feature_strength,
        "training_count": int(len(train_idx)),
        "positive_count": int(np.sum(y_train == 1)),
        "negative_count": int(np.sum(y_train == 0)),
    }
    state["row_context_cache"][row_index] = context
    return context


def _case_trade_return(
    touched_side: str | None,
    predicted_direction: str | None,
    signal_close: float,
    outcome_close: float,
    atr: float,
) -> dict:
    side_sign = features._side_sign(touched_side)
    if predicted_direction == "continuation":
        trade_sign = side_sign
    elif predicted_direction == "reversal":
        trade_sign = -side_sign
    else:
        trade_sign = 0.0

    if (
        abs(trade_sign) <= settings._EPS
        or not np.isfinite(signal_close)
        or signal_close <= 0
        or not np.isfinite(outcome_close)
    ):
        return {"trade_direction": None, "trade_return": None, "trade_return_atr": None}

    price_delta = outcome_close - signal_close
    trade_return = trade_sign * (price_delta / signal_close)
    trade_return_atr = None
    if np.isfinite(atr) and atr > 0:
        trade_return_atr = trade_sign * (price_delta / atr)
    return {
        "trade_direction": "long" if trade_sign > 0 else "short",
        "trade_return": round(float(trade_return), 6),
        "trade_return_atr": round(float(trade_return_atr), 6) if trade_return_atr is not None else None,
    }


def _similar_case_payload(
    feature_df: pd.DataFrame,
    idx: int,
    horizon: int,
    predicted_direction: str,
    *,
    distance: float | None = None,
    similarity: float | None = None,
) -> dict | None:
    outcome_idx = int(idx) + int(horizon)
    if idx < 0 or outcome_idx >= len(feature_df):
        return None

    row = feature_df.iloc[int(idx)]
    outcome_row = feature_df.iloc[outcome_idx]
    raw_touched_side = row.get("touched_side")
    touched_side = raw_touched_side if raw_touched_side in ("Upper", "Lower") else model_utils._training_side_for_row(row)
    signal_close = features._safe_num(row.get("close"), np.nan)
    outcome_close = features._safe_num(outcome_row.get("close"), np.nan)
    atr = features._safe_num(row.get("ATR14"), np.nan)
    actual_direction = (
        features._actual_direction(touched_side, signal_close, outcome_close, features._flat_tolerance_for_row(row))
        if touched_side in ("Upper", "Lower")
        else None
    )
    payload = {
        "signal_date": features._to_date_string(row.get("date")),
        "outcome_date": features._to_date_string(outcome_row.get("date")),
        "horizon_days": int(horizon),
        "touched_side": touched_side,
        "was_band_touch": raw_touched_side in ("Upper", "Lower"),
        "predicted_direction": predicted_direction,
        "actual_direction": actual_direction,
        "signal_close": round(signal_close, 6) if np.isfinite(signal_close) else None,
        "outcome_close": round(outcome_close, 6) if np.isfinite(outcome_close) else None,
        "is_correct": bool(features._prediction_is_correct(predicted_direction, actual_direction)),
        **_case_trade_return(touched_side, predicted_direction, signal_close, outcome_close, atr),
    }
    if distance is not None:
        payload["distance"] = round(float(distance), 6)
    if similarity is not None:
        payload["similarity"] = round(float(similarity), 6)
    return payload


def _adaptive_raw_profile_prediction(
    feature_df: pd.DataFrame,
    row_index: int,
    horizon: int,
    profile: dict,
) -> dict | None:
    state = _adaptive_state(feature_df, horizon)
    cache_key = (int(row_index), _adaptive_profile_key(profile))
    if cache_key in state["raw_cache"]:
        return state["raw_cache"][cache_key]

    feature_columns = state["feature_columns"]
    context = _adaptive_row_context(feature_df, row_index, horizon)
    if context is None:
        state["raw_cache"][cache_key] = None
        return None

    train_idx = context["train_idx"]
    y_train = context["y_train"]
    z_train = context["z_train"]
    z_target = context["z_target"]
    feature_strength = context["feature_strength"]
    top_count = min(max(1, int(profile["top_features"])), len(feature_columns))
    selected_indices = np.argsort(feature_strength)[-top_count:]

    analog_mask = np.ones(len(train_idx), dtype=bool)
    if profile.get("same_side"):
        target_side = np.sign(state["side_sign"][row_index])
        same_side_mask = np.sign(state["side_sign"][train_idx]) == target_side
        if int(same_side_mask.sum()) >= 40:
            analog_mask = same_side_mask

    analog_z = z_train[analog_mask]
    analog_y = y_train[analog_mask]
    analog_idx = train_idx[analog_mask]
    neighbor_count = min(int(profile["neighbors"]), len(analog_idx))
    if neighbor_count <= 0:
        state["raw_cache"][cache_key] = None
        return None

    distances = np.mean((analog_z[:, selected_indices] - z_target[selected_indices]) ** 2, axis=1)
    order = np.argsort(distances)[:neighbor_count]
    neighbor_idx = analog_idx[order]
    neighbor_labels = analog_y[order]
    neighbor_distances = distances[order]
    similarity = np.exp(-neighbor_distances / 2.0)
    age_scale = max(1, int(analog_idx.max()) - int(analog_idx.min()))
    recency = (neighbor_idx - int(analog_idx.min())) / age_scale
    weights = similarity * (1.0 + (0.80 * recency)) * (1.0 + (0.35 * state["touch_weight"][neighbor_idx]))
    continuation_probability = features._clamp(float(np.dot(weights, neighbor_labels) / (weights.sum() + settings._EPS)), 0.01, 0.99)
    reversal_probability = 1.0 - continuation_probability
    if continuation_probability >= reversal_probability:
        direction = "continuation"
        confidence = continuation_probability
        required_confidence = float(profile["continuation_confidence"])
    else:
        direction = "reversal"
        confidence = reversal_probability
        required_confidence = float(profile["reversal_confidence"])

    expected_label = 1 if direction == "continuation" else 0
    analog_agreement = float(np.mean(neighbor_labels == expected_label))
    selected_features = sorted(
        (
            {
                "feature": feature_columns[int(feature_idx)],
                "strength": round(float(feature_strength[int(feature_idx)]), 6),
                "target_value": features._value_for_payload(state["x"][row_index, int(feature_idx)]),
            }
            for feature_idx in selected_indices
        ),
        key=lambda item: item["strength"],
        reverse=True,
    )

    neighbors = []
    for idx, distance, case_similarity in zip(neighbor_idx[:8], neighbor_distances[:8], similarity[:8]):
        case = _similar_case_payload(
            feature_df,
            int(idx),
            horizon,
            direction,
            distance=float(distance),
            similarity=float(case_similarity),
        )
        if case is not None:
            neighbors.append(case)

    result = {
        "profile": profile,
        "direction": direction,
        "continuation_probability": continuation_probability,
        "reversal_probability": reversal_probability,
        "confidence": confidence,
        "required_confidence": required_confidence,
        "analog_agreement": analog_agreement,
        "status": confidence >= required_confidence and analog_agreement >= required_confidence,
        "neighbor_count": int(neighbor_count),
        "training_count": context["training_count"],
        "positive_count": context["positive_count"],
        "negative_count": context["negative_count"],
        "selected_features": selected_features[:10],
        "neighbors": neighbors,
    }
    state["raw_cache"][cache_key] = result
    return result


def _adaptive_reversal_veto_reason(row: pd.Series, horizon: int) -> str | None:
    side = row.get("touched_side")
    side_ret_5d = features._safe_num(row.get("side_ret_5d"), 0.0)
    side_ret_10d = features._safe_num(row.get("side_ret_10d"), 0.0)
    side_qqq = features._safe_num(row.get("side_qqq_ret_5d"), 0.0)
    side_xlk = features._safe_num(row.get("side_xlk_ret_5d"), 0.0)
    pressure = features._safe_num(row.get("side_weighted_volume_pressure_5"), 0.0)
    adx = features._safe_num(row.get("ADX14"), 0.0)
    band_rank = features._safe_num(row.get("band_width_percentile"), 0.5)
    bandwidth_change = features._safe_num(row.get("bandwidth_change_5d"), 0.0)
    consecutive = features._safe_num(row.get("consecutive_touch_count"), 0.0)
    reentry = features._safe_num(row.get("touch_reentry_signal"), 0.0)
    wick_minus_body = features._safe_num(row.get("touch_wick_minus_body"), 0.0)
    close_location = features._safe_num(row.get("side_close_location"), 0.0)
    touch_depth = features._safe_num(row.get("touch_depth_atr"), 0.0)

    if side == "Upper":
        broad_breakout = side_qqq > 0.035 and side_xlk > 0.035 and side_ret_5d > 0.045 and pressure > 0.10
        if broad_breakout and adx < 24.0:
            return "broad_breakout_without_exhaustion"
        if adx < 20.0 and pressure > 0.15 and touch_depth < 0.50 and side_ret_10d < 0.07:
            return "early_breakout_no_exhaustion"
        if (
            band_rank > 0.75
            and bandwidth_change > 0.08
            and side_ret_10d > 0.12
            and consecutive >= 3
            and pressure > 0.30
        ):
            return "expanding_band_breakout"
        clean_close_outside = reentry < 0.0 and wick_minus_body < -0.25 and close_location > 0.50
        if clean_close_outside and adx < 24.0 and not (band_rank > 0.85 and side_ret_10d > 0.15):
            return "no_rejection_breakout_close"

    if side == "Lower":
        falling_thrust = side_ret_5d > 0.04 or side_ret_10d > 0.06
        weak_exhaustion = pressure < 0.20 and wick_minus_body < 0.25 and touch_depth < 0.70
        early_or_outside = consecutive < 6 or reentry < 0.0
        market_not_helping = side_qqq > 0.005 or side_xlk > 0.005
        if falling_thrust and weak_exhaustion and early_or_outside and market_not_helping:
            return "falling_knife_no_exhaustion"

    return None


def _adaptive_continuation_veto_reason(row: pd.Series, horizon: int) -> str | None:
    side = row.get("touched_side")
    side_ret_5d = features._safe_num(row.get("side_ret_5d"), 0.0)
    side_ret_10d = features._safe_num(row.get("side_ret_10d"), 0.0)
    side_ret_20d = features._safe_num(row.get("side_ret_20d"), 0.0)
    side_rsi = features._safe_num(row.get("side_rsi_deviation"), 0.0)
    side_qqq = features._safe_num(row.get("side_qqq_ret_5d"), 0.0)
    pressure = features._safe_num(row.get("side_weighted_volume_pressure_5"), 0.0)
    rel_volume = features._safe_num(row.get("rel_volume_20"), 1.0)
    band_rank = features._safe_num(row.get("band_width_percentile"), 0.5)
    bandwidth_change = features._safe_num(row.get("bandwidth_change_5d"), 0.0)
    consecutive = features._safe_num(row.get("consecutive_touch_count"), 0.0)
    reentry = features._safe_num(row.get("touch_reentry_signal"), 0.0)
    wick_minus_body = features._safe_num(row.get("touch_wick_minus_body"), 0.0)
    close_location = features._safe_num(row.get("side_close_location"), 0.0)

    rejection_close = reentry > 0.0 and wick_minus_body >= 0.28 and close_location <= 0.25
    if rejection_close:
        return "continuation_rejected_at_band"

    if side == "Upper":
        overextended = side_ret_10d > 0.12 or side_ret_20d > 0.18
        exhaustion = side_rsi > 0.70 and (wick_minus_body >= 0.18 or pressure < -0.05)
        fragile_squeeze_release = band_rank > 0.85 and bandwidth_change < 0.02 and pressure < 0.10
        if horizon >= 10 and overextended and exhaustion and not fragile_squeeze_release:
            return "upper_exhaustion_reversal_risk"

    if side == "Lower":
        capitulation = (
            consecutive >= 4
            and side_ret_5d > 0.045
            and (pressure >= 0.25 or rel_volume >= 1.30)
            and wick_minus_body >= 0.12
            and side_qqq <= 0.015
        )
        if capitulation:
            return "lower_capitulation_reversal_risk"

    return None


def _adaptive_profile_thresholds(profile: dict, direction: str) -> tuple[float, float, float]:
    min_precision = (
        settings._ADAPTIVE_REVERSAL_PRECISION_THRESHOLD
        if direction == "reversal"
        else settings._ADAPTIVE_CONTINUATION_PRECISION_THRESHOLD
    )
    min_wilson = settings._ADAPTIVE_MIN_WILSON
    min_confidence = float(
        profile["reversal_confidence"] if direction == "reversal" else profile["continuation_confidence"]
    )
    tier = profile.get("tier", "core")

    if tier == "expansion":
        if direction == "reversal":
            min_precision = max(min_precision, settings._ADAPTIVE_EXPANSION_REVERSAL_MIN_PRECISION)
            min_wilson = max(min_wilson, settings._ADAPTIVE_EXPANSION_REVERSAL_MIN_WILSON)
            min_confidence = max(min_confidence, settings._ADAPTIVE_EXPANSION_REVERSAL_MIN_CONFIDENCE)
        else:
            min_precision = max(min_precision, settings._ADAPTIVE_EXPANSION_CONTINUATION_MIN_PRECISION)
            min_wilson = min(min_wilson, settings._ADAPTIVE_EXPANSION_CONTINUATION_MIN_WILSON)
            min_confidence = max(min_confidence, settings._ADAPTIVE_EXPANSION_CONTINUATION_MIN_CONFIDENCE)
    elif tier == "opportunity":
        if direction != "continuation":
            return 1.01, 1.01, 1.01
        min_precision = max(min_precision, settings._ADAPTIVE_OPPORTUNITY_CONTINUATION_MIN_PRECISION)
        min_wilson = min(min_wilson, settings._ADAPTIVE_OPPORTUNITY_CONTINUATION_MIN_WILSON)
        min_confidence = max(min_confidence, settings._ADAPTIVE_OPPORTUNITY_CONTINUATION_MIN_CONFIDENCE)

    return min_precision, min_wilson, min_confidence


def _empirical_bucket(value: float, cuts: tuple[float, float, float, float]) -> str:
    if value <= cuts[0]:
        return "very_low"
    if value <= cuts[1]:
        return "low"
    if value <= cuts[2]:
        return "neutral"
    if value <= cuts[3]:
        return "high"
    return "very_high"


def _empirical_regime_attrs(row: pd.Series, horizon: int) -> dict[str, str]:
    side = row.get("touched_side")
    reentry = features._safe_num(row.get("touch_reentry_signal"), 0.0)
    wick_minus_body = features._safe_num(row.get("touch_wick_minus_body"), 0.0)
    close_location = features._safe_num(row.get("side_close_location"), 0.0)
    consecutive = features._safe_num(row.get("consecutive_touch_count"), 0.0)
    side_ret_5d = features._safe_num(row.get("side_ret_5d"), 0.0)
    side_ret_10d = features._safe_num(row.get("side_ret_10d"), 0.0)
    side_ret_20d = features._safe_num(row.get("side_ret_20d"), 0.0)
    side_qqq = features._safe_num(row.get("side_qqq_ret_5d"), 0.0)
    side_xlk = features._safe_num(row.get("side_xlk_ret_5d"), 0.0)
    pressure = features._safe_num(row.get("side_weighted_volume_pressure_5"), 0.0)
    rel_volume = features._safe_num(row.get("rel_volume_20"), 1.0)
    band_rank = features._safe_num(row.get("band_width_percentile"), 0.5)
    bandwidth_change = features._safe_num(row.get("bandwidth_change_5d"), 0.0)
    touch_depth = features._safe_num(row.get("touch_depth_atr"), 0.0)
    adx = features._safe_num(row.get("ADX14"), 0.0)

    if reentry > 0.0 and (wick_minus_body >= 0.10 or close_location <= 0.30):
        touch_quality = "rejection"
    elif reentry <= 0.0 and close_location >= 0.35 and wick_minus_body < 0.18:
        touch_quality = "outside_close"
    elif touch_depth >= 0.60:
        touch_quality = "deep_touch"
    else:
        touch_quality = "mixed_touch"

    if consecutive >= 5:
        cluster = "extended_walk"
    elif consecutive >= 3:
        cluster = "walk"
    elif consecutive >= 2:
        cluster = "second_touch"
    else:
        cluster = "single_touch"

    trend_value = side_ret_10d if horizon >= 10 else side_ret_5d
    if horizon >= 10:
        trend = _empirical_bucket(trend_value, (-0.025, 0.015, 0.055, 0.105))
    else:
        trend = _empirical_bucket(trend_value, (-0.015, 0.010, 0.035, 0.075))

    longer_trend = _empirical_bucket(side_ret_20d, (-0.040, 0.010, 0.070, 0.145))
    market_value = (side_qqq + side_xlk) / 2.0
    market = _empirical_bucket(market_value, (-0.020, -0.004, 0.008, 0.025))
    volume_pressure = _empirical_bucket(pressure, (-0.25, -0.05, 0.12, 0.32))
    participation = "high_volume" if rel_volume >= 1.25 else "dry_volume" if rel_volume <= 0.80 else "normal_volume"
    band_state = (
        "wide_expanding"
        if band_rank >= 0.75 and bandwidth_change >= 0.02
        else "wide_stalling"
        if band_rank >= 0.75
        else "compressed"
        if band_rank <= 0.30
        else "normal_band"
    )
    adx_state = "strong_trend" if adx >= 28 else "weak_trend" if adx <= 18 else "moderate_trend"

    return {
        "side": str(side),
        "touch_quality": touch_quality,
        "cluster": cluster,
        "trend": trend,
        "longer_trend": longer_trend,
        "market": market,
        "volume_pressure": volume_pressure,
        "participation": participation,
        "band_state": band_state,
        "adx_state": adx_state,
    }


def _empirical_regime_specs(horizon: int) -> list[tuple[str, tuple[str, ...], int]]:
    base = [
        ("side_touch_cluster_trend", ("side", "touch_quality", "cluster", "trend"), 4),
        ("side_touch_cluster", ("side", "touch_quality", "cluster"), 5),
        ("side_touch_trend", ("side", "touch_quality", "trend"), 5),
        ("side_cluster_trend", ("side", "cluster", "trend"), 5),
        ("side_touch_market", ("side", "touch_quality", "market"), 5),
        ("side_trend_market", ("side", "trend", "market"), 6),
        ("side_touch", ("side", "touch_quality"), 6),
        ("side_cluster", ("side", "cluster"), 7),
        ("side_trend", ("side", "trend"), 7),
        ("side_market", ("side", "market"), 8),
        ("side_only", ("side",), 8),
    ]
    if horizon >= 10:
        return base + [
            ("side_longer_trend", ("side", "longer_trend"), 8),
            ("side_band_state", ("side", "band_state"), 8),
        ]
    return base


def _empirical_rows_for_scope(
    feature_df: pd.DataFrame,
    target_attrs: dict[str, str],
    *,
    row_index: int,
    horizon: int,
    fields: tuple[str, ...],
) -> list[dict]:
    max_idx = min(row_index - horizon, len(feature_df) - horizon - 1)
    if max_idx < 0:
        return []

    matches: list[dict] = []
    for idx in range(max_idx + 1):
        row = feature_df.iloc[idx]
        if features._safe_bool(row.get("event_risk_blocked")):
            continue
        if row.get("touched_side") not in ("Upper", "Lower"):
            continue
        attrs = _empirical_regime_attrs(row, horizon)
        if any(attrs.get(field) != target_attrs.get(field) for field in fields):
            continue
        actual = _actual_direction_for_index(feature_df, idx, horizon)
        if actual not in ("continuation", "reversal"):
            continue
        outcome_row = feature_df.iloc[idx + horizon]
        signal_close = features._safe_num(row.get("close"), np.nan)
        outcome_close = features._safe_num(outcome_row.get("close"), np.nan)
        matches.append(
            {
                "idx": idx,
                "signal_date": features._to_date_string(row.get("date")),
                "outcome_date": features._to_date_string(outcome_row.get("date")),
                "direction": actual,
                "actual_direction": actual,
                "touched_side": row.get("touched_side"),
                "signal_close": round(signal_close, 6) if np.isfinite(signal_close) else None,
                "outcome_close": round(outcome_close, 6) if np.isfinite(outcome_close) else None,
                "horizon_days": int(horizon),
            }
        )
    return matches[-settings._EMPIRICAL_REGIME_MAX_MATCHES:]


def _empirical_case_for_direction(row: dict, direction: str) -> dict:
    signal_close = features._safe_num(row.get("signal_close"), np.nan)
    outcome_close = features._safe_num(row.get("outcome_close"), np.nan)
    return {
        "signal_date": row.get("signal_date") or row.get("date"),
        "outcome_date": row.get("outcome_date"),
        "horizon_days": row.get("horizon_days"),
        "touched_side": row.get("touched_side"),
        "predicted_direction": direction,
        "actual_direction": row.get("actual_direction") or row.get("direction"),
        "signal_close": row.get("signal_close"),
        "outcome_close": row.get("outcome_close"),
        "is_correct": bool(features._prediction_is_correct(direction, row.get("actual_direction") or row.get("direction"))),
        **_case_trade_return(
            row.get("touched_side"),
            direction,
            signal_close,
            outcome_close,
            np.nan,
        ),
    }


def _score_empirical_regime_direction(
    rows: list[dict],
    *,
    direction: str,
    min_matches: int,
    scope: str,
    fields: tuple[str, ...],
) -> dict | None:
    count = len(rows)
    if count < min_matches:
        return None
    correct = sum(1 for row in rows if row.get("direction") == direction)
    precision = correct / count
    posterior = (correct + 1.0) / (count + 2.0)
    wilson = model_utils._wilson_lower_bound(correct, count)
    recent_rows = rows[-min(settings._EMPIRICAL_REGIME_RECENT_MATCHES, count) :]
    recent_correct = sum(1 for row in recent_rows if row.get("direction") == direction)
    recent_precision = recent_correct / len(recent_rows) if recent_rows else precision

    if direction == "reversal":
        min_precision = settings._EMPIRICAL_REVERSAL_MIN_PRECISION
        min_wilson = settings._EMPIRICAL_REVERSAL_MIN_WILSON
        min_recent = 0.78
    else:
        min_precision = settings._EMPIRICAL_CONTINUATION_MIN_PRECISION
        min_wilson = settings._EMPIRICAL_CONTINUATION_MIN_WILSON
        min_recent = 0.72

    if precision < min_precision or wilson < min_wilson or recent_precision < min_recent:
        return None

    specificity = min(1.0, len(fields) / 4.0)
    sample_strength = min(1.0, math.log1p(count) / math.log1p(settings._EMPIRICAL_REGIME_MAX_MATCHES))
    score = (
        (0.42 * precision)
        + (0.24 * wilson)
        + (0.18 * recent_precision)
        + (0.08 * posterior)
        + (0.05 * sample_strength)
        + (0.03 * specificity)
    )
    return {
        "status": "ready",
        "source": "empirical_regime",
        "direction": direction,
        "scope": scope,
        "fields": list(fields),
        "match_count": count,
        "correct_count": correct,
        "precision": precision,
        "posterior_probability": posterior,
        "wilson_lower_bound": wilson,
        "recent_precision": recent_precision,
        "score": score,
        "neighbors": [_empirical_case_for_direction(row, direction) for row in rows[-8:]],
    }


def _score_recent_empirical_regime_direction(
    rows: list[dict],
    *,
    direction: str,
    scope: str,
    fields: tuple[str, ...],
) -> list[dict]:
    if direction not in ("continuation", "reversal"):
        return []

    out: list[dict] = []
    min_precision = (
        settings._EMPIRICAL_RECENT_REVERSAL_MIN_PRECISION
        if direction == "reversal"
        else settings._EMPIRICAL_RECENT_CONTINUATION_MIN_PRECISION
    )
    min_wilson = (
        settings._EMPIRICAL_RECENT_REVERSAL_MIN_WILSON
        if direction == "reversal"
        else settings._EMPIRICAL_RECENT_CONTINUATION_MIN_WILSON
    )

    for window in settings._EMPIRICAL_RECENT_WINDOWS:
        if len(rows) < window:
            continue
        recent_rows = rows[-window:]
        correct = sum(1 for row in recent_rows if row.get("direction") == direction)
        precision = correct / window
        wilson = model_utils._wilson_lower_bound(correct, window)
        if precision < min_precision or wilson < min_wilson:
            continue
        posterior = (correct + 1.0) / (window + 2.0)
        full_correct = sum(1 for row in rows if row.get("direction") == direction)
        full_precision = full_correct / len(rows) if rows else precision
        specificity = min(1.0, len(fields) / 4.0)
        score = (
            (0.45 * precision)
            + (0.22 * wilson)
            + (0.14 * posterior)
            + (0.10 * min(1.0, window / 12.0))
            + (0.06 * specificity)
            + (0.03 * full_precision)
        )
        out.append(
            {
                "status": "ready",
                "source": "recent_empirical_regime",
                "direction": direction,
                "scope": f"recent_{scope}_{window}",
                "base_scope": scope,
                "fields": list(fields),
                "match_count": window,
                "full_match_count": len(rows),
                "correct_count": correct,
                "precision": precision,
                "posterior_probability": posterior,
                "wilson_lower_bound": wilson,
                "recent_precision": precision,
                "full_precision": full_precision,
                "score": score,
                "neighbors": [_empirical_case_for_direction(row, direction) for row in recent_rows[-8:]],
            }
        )
    return out


def _score_empirical_regime(feature_df: pd.DataFrame, row_index: int, horizon: int) -> dict | None:
    row = feature_df.iloc[row_index]
    if row.get("touched_side") not in ("Upper", "Lower"):
        return None
    blocked_directions = {
        direction
        for direction, reason in (
            ("continuation", _adaptive_continuation_veto_reason(row, horizon)),
            ("reversal", _adaptive_reversal_veto_reason(row, horizon)),
        )
        if reason is not None
    }
    target_attrs = _empirical_regime_attrs(row, horizon)
    candidates: list[dict] = []
    for scope, fields, min_matches in _empirical_regime_specs(horizon):
        rows = _empirical_rows_for_scope(
            feature_df,
            target_attrs,
            row_index=row_index,
            horizon=horizon,
            fields=fields,
        )
        reverse_min_matches = max(min_matches, settings._EMPIRICAL_REVERSAL_MIN_MATCHES)
        continue_min_matches = max(min_matches, settings._EMPIRICAL_CONTINUATION_MIN_MATCHES)
        for direction, required_matches in (
            ("reversal", reverse_min_matches),
            ("continuation", continue_min_matches),
        ):
            if direction in blocked_directions:
                continue
            scored = _score_empirical_regime_direction(
                rows,
                direction=direction,
                min_matches=required_matches,
                scope=scope,
                fields=fields,
            )
            if scored is not None:
                candidates.append(scored)
            candidates.extend(
                _score_recent_empirical_regime_direction(
                    rows,
                    direction=direction,
                    scope=scope,
                    fields=fields,
                )
            )

    if not candidates:
        return None

    return sorted(
        candidates,
        key=lambda item: (
            item["score"],
            item["precision"],
            item["match_count"],
            1 if item["direction"] == "reversal" else 0,
        ),
        reverse=True,
    )[0]


def _empirical_regime_horizon(
    feature_df: pd.DataFrame,
    row_index: int,
    horizon: int,
    signal: dict,
    *,
    training_count: int,
    continuation_count: int,
    reversal_count: int,
    candidate_count: int,
) -> dict:
    direction = signal["direction"]
    posterior = features._clamp(features._safe_num(signal.get("posterior_probability"), 0.5), 0.01, 0.99)
    if direction == "continuation":
        continuation_probability = posterior
        reversal_probability = 1.0 - posterior
    else:
        reversal_probability = posterior
        continuation_probability = 1.0 - posterior

    confidence_score = int(round(max(continuation_probability, reversal_probability) * 100))
    model = model_utils._model_metadata(
        training_count,
        continuation_count,
        reversal_count,
        validation_accuracy=signal["precision"],
        validation_precision=signal["precision"],
        candidate_count=candidate_count,
    )
    model["type"] = "walk_forward_empirical_regime"
    playbook = {
        "id": f"empirical_{signal['scope']}",
        "name": "Empirical Regime Tape",
        "direction": direction,
        "tier": "regime",
        "match_count": signal["match_count"],
        "correct_count": signal["correct_count"],
        "precision": round(signal["precision"], 6),
        "posterior_probability": round(signal["posterior_probability"], 6),
        "wilson_lower_bound": round(signal["wilson_lower_bound"], 6),
        "recent_precision": round(signal["recent_precision"], 6),
        "score": round(signal["score"], 6),
        "scope": signal["scope"],
        "fields": signal["fields"],
        "neighbors": signal["neighbors"],
    }
    return {
        "status": "prediction",
        "predicted_direction": direction,
        "continuation_probability": round(continuation_probability, 6),
        "reversal_probability": round(reversal_probability, 6),
        "continuation_confidence_score": int(round(continuation_probability * 100)),
        "reversal_confidence_score": int(round(reversal_probability * 100)),
        "continuation_validation_precision": signal["precision"] if direction == "continuation" else None,
        "reversal_validation_precision": signal["precision"] if direction == "reversal" else None,
        "continuation_validation_count": signal["match_count"] if direction == "continuation" else 0,
        "reversal_validation_count": signal["match_count"] if direction == "reversal" else 0,
        "validation_policy": {
            "empirical_reversal_min_precision": settings._EMPIRICAL_REVERSAL_MIN_PRECISION,
            "empirical_continuation_min_precision": settings._EMPIRICAL_CONTINUATION_MIN_PRECISION,
            "empirical_reversal_min_wilson": settings._EMPIRICAL_REVERSAL_MIN_WILSON,
            "empirical_continuation_min_wilson": settings._EMPIRICAL_CONTINUATION_MIN_WILSON,
            "empirical_recent_reversal_min_precision": settings._EMPIRICAL_RECENT_REVERSAL_MIN_PRECISION,
            "empirical_recent_continuation_min_precision": settings._EMPIRICAL_RECENT_CONTINUATION_MIN_PRECISION,
            "scope": signal["scope"],
            "fields": signal["fields"],
        },
        "confidence_score": confidence_score,
        "threshold": settings._PREDICTION_THRESHOLD,
        "no_prediction_reason": None,
        "reversal_veto_reason": None,
        "analog_evidence": {
            "status": "ready",
            "direction": direction,
            "posterior_probability": playbook["posterior_probability"],
            "neighbor_count": signal["match_count"],
            "precision": playbook["precision"],
            "lower_bound": playbook["wilson_lower_bound"],
            "recent_precision": playbook["recent_precision"],
        },
        "analog_override": False,
        "deployment_quality_gate": None,
        "blocked_prediction": None,
        "playbook": playbook,
        "adaptive_candidates": [
            {
                "profile_id": playbook["id"],
                "status": "ready",
                "direction": direction,
                "tier": "regime",
                "precision": playbook["precision"],
                "validation_count": signal["match_count"],
                "confidence": round(max(continuation_probability, reversal_probability), 6),
                "blocked_reason": None,
            }
        ],
        "contributions": [
            {
                "horizon": f"{horizon}d",
                "feature": playbook["name"],
                "value": signal["match_count"],
                "impact": direction,
                "contribution": playbook["score"],
            }
        ],
        "model": model,
    }


def _adaptive_validation_indices(feature_df: pd.DataFrame, row_index: int, horizon: int) -> list[int]:
    start = max(0, row_index - settings._ADAPTIVE_VALIDATION_LOOKBACK_ROWS)
    indices: list[int] = []
    for idx in range(start, row_index):
        row = feature_df.iloc[idx]
        if row.get("touched_side") not in ("Upper", "Lower"):
            continue
        if features._safe_bool(row.get("event_risk_blocked")):
            continue
        if idx + horizon > row_index:
            continue
        if _actual_direction_for_index(feature_df, idx, horizon) not in ("continuation", "reversal"):
            continue
        indices.append(idx)
    return indices[-settings._ADAPTIVE_MAX_VALIDATION_TOUCHES:]


def _score_adaptive_profile(
    feature_df: pd.DataFrame,
    row_index: int,
    horizon: int,
    profile: dict,
) -> dict | None:
    raw = _adaptive_raw_profile_prediction(feature_df, row_index, horizon, profile)
    if raw is None or not raw["status"]:
        return None

    direction = raw["direction"]
    allowed_directions = profile.get("allowed_directions")
    if allowed_directions and direction not in allowed_directions:
        return {
            "status": "blocked",
            "blocked_reason": "profile_direction_filter",
            "raw": raw,
            "profile": profile,
        }

    if direction == "continuation":
        veto_reason = _adaptive_continuation_veto_reason(feature_df.iloc[row_index], horizon)
        if veto_reason is not None:
            return {
                "status": "blocked",
                "blocked_reason": veto_reason,
                "raw": raw,
                "profile": profile,
            }

    if direction == "reversal":
        veto_reason = _adaptive_reversal_veto_reason(feature_df.iloc[row_index], horizon)
        if veto_reason is not None:
            return {
                "status": "blocked",
                "blocked_reason": veto_reason,
                "raw": raw,
                "profile": profile,
            }

    validation_count = int(raw["neighbor_count"])
    correct_count = int(round(raw["analog_agreement"] * validation_count))
    precision = raw["analog_agreement"]
    wilson = model_utils._wilson_lower_bound(correct_count, validation_count) if validation_count else 0.0
    min_precision, min_wilson, min_confidence = _adaptive_profile_thresholds(profile, direction)
    passed = (
        validation_count > 0
        and precision >= min_precision
        and wilson >= min_wilson
        and raw["confidence"] >= min_confidence
    )
    recent_precision = precision
    score = (
        (0.50 * (precision or 0.0))
        + (0.25 * wilson)
        + (0.20 * raw["confidence"])
        + (0.05 * min(validation_count, 12) / 12.0)
    )
    return {
        "status": "ready" if passed else "unvalidated",
        "blocked_reason": None,
        "profile": profile,
        "raw": raw,
        "direction": direction,
        "validation_count": validation_count,
        "correct_count": correct_count,
        "precision": precision,
        "wilson_lower_bound": wilson,
        "recent_precision": recent_precision,
        "score": score,
        "tier": profile.get("tier", "core"),
    }


def _adaptive_training_summary(feature_df: pd.DataFrame, row_index: int, horizon: int) -> tuple[int, int, int]:
    state = _adaptive_state(feature_df, horizon)
    train_idx = _adaptive_training_indices(state, row_index, horizon)
    if len(train_idx) == 0:
        return 0, 0, 0
    labels = state["labels"][train_idx].astype(int)
    return len(labels), int(np.sum(labels == 1)), int(np.sum(labels == 0))


def _evaluate_horizon_with_adaptive_analogs(feature_df: pd.DataFrame, row_index: int, horizon: int) -> dict:
    training_count, continuation_count, reversal_count = _adaptive_training_summary(feature_df, row_index, horizon)
    profile_count = len(_adaptive_profiles_for_horizon(horizon))
    regime_count = len(_empirical_regime_specs(horizon))
    candidate_search_count = profile_count + regime_count
    if training_count < settings._ADAPTIVE_MIN_TRAINING_ROWS:
        result = model_utils._no_prediction_horizon(
            horizon,
            "insufficient_training_data",
            training_sample_count=training_count,
            positive_count=continuation_count,
            negative_count=reversal_count,
        )
        result["model"]["type"] = "walk_forward_adaptive_analog"
        result["model"]["candidate_search_count"] = candidate_search_count
        result["adaptive_candidates"] = []
        return result

    regime_signal = _score_empirical_regime(feature_df, row_index, horizon)
    if regime_signal is not None:
        return _empirical_regime_horizon(
            feature_df,
            row_index,
            horizon,
            regime_signal,
            training_count=training_count,
            continuation_count=continuation_count,
            reversal_count=reversal_count,
            candidate_count=candidate_search_count,
        )

    scored = [
        score
        for profile in _adaptive_profiles_for_horizon(horizon)
        for score in [_score_adaptive_profile(feature_df, row_index, horizon, profile)]
        if score is not None
    ]
    ready = [score for score in scored if score["status"] == "ready"]
    if not ready:
        blocked = [score for score in scored if score.get("status") == "blocked"]
        raw_hits = [score for score in scored if score.get("raw", {}).get("status")]
        reason = "unvalidated_adaptive_edge"
        if blocked and not any(score.get("status") == "unvalidated" for score in scored):
            reason = blocked[0].get("blocked_reason") or "reversal_veto"
        elif not raw_hits:
            reason = "low_adaptive_confidence"

        result = model_utils._no_prediction_horizon(
            horizon,
            reason,
            training_sample_count=training_count,
            positive_count=continuation_count,
            negative_count=reversal_count,
        )
        result["model"]["type"] = "walk_forward_adaptive_analog"
        result["model"]["candidate_search_count"] = candidate_search_count
        result["adaptive_candidates"] = [
            {
                "profile_id": item["profile"]["id"],
                "status": item["status"],
                "direction": item.get("direction") or item.get("raw", {}).get("direction"),
                "tier": item.get("tier") or item["profile"].get("tier"),
                "precision": features._value_for_payload(item.get("precision")),
                "validation_count": item.get("validation_count", 0),
                "blocked_reason": item.get("blocked_reason"),
                "confidence": features._value_for_payload(item.get("raw", {}).get("confidence")),
            }
            for item in sorted(scored, key=lambda candidate: candidate.get("score", 0.0), reverse=True)[:5]
        ]
        return result

    best = sorted(
        ready,
        key=lambda item: (item["score"], item["validation_count"], item["raw"]["confidence"]),
        reverse=True,
    )[0]
    raw = best["raw"]
    direction = best["direction"]
    continuation_probability = raw["continuation_probability"]
    reversal_probability = raw["reversal_probability"]
    confidence_score = int(round(max(continuation_probability, reversal_probability) * 100))
    precision = best["precision"] or 0.0
    model = model_utils._model_metadata(
        training_count,
        continuation_count,
        reversal_count,
        validation_accuracy=precision,
        validation_precision=precision,
        candidate_count=len(scored),
    )
    model["type"] = "walk_forward_adaptive_analog"
    model["candidate_search_count"] = candidate_search_count
    signal = {
        "id": f"adaptive_{best['profile']['id']}",
        "name": "Adaptive Analog Signal",
        "direction": direction,
        "tier": best.get("tier", best["profile"].get("tier", "core")),
        "match_count": best["validation_count"],
        "correct_count": best["correct_count"],
        "precision": round(precision, 6),
        "posterior_probability": round((best["correct_count"] + 1.0) / (best["validation_count"] + 2.0), 6),
        "wilson_lower_bound": round(best["wilson_lower_bound"], 6),
        "recent_precision": round(best["recent_precision"], 6) if best["recent_precision"] is not None else None,
        "score": round(best["score"], 6),
        "profile": deepcopy(best["profile"]),
        "selected_features": raw["selected_features"],
        "neighbors": raw["neighbors"],
    }
    selected_features = raw["selected_features"][:8]
    return {
        "status": "prediction",
        "predicted_direction": direction,
        "continuation_probability": round(continuation_probability, 6),
        "reversal_probability": round(reversal_probability, 6),
        "continuation_confidence_score": int(round(continuation_probability * 100)),
        "reversal_confidence_score": int(round(reversal_probability * 100)),
        "continuation_validation_precision": precision if direction == "continuation" else None,
        "reversal_validation_precision": precision if direction == "reversal" else None,
        "continuation_validation_count": best["validation_count"] if direction == "continuation" else 0,
        "reversal_validation_count": best["validation_count"] if direction == "reversal" else 0,
        "validation_policy": {
            "min_reversal_precision": settings._ADAPTIVE_REVERSAL_PRECISION_THRESHOLD,
            "min_continuation_precision": settings._ADAPTIVE_CONTINUATION_PRECISION_THRESHOLD,
            "min_wilson_lower_bound": settings._ADAPTIVE_MIN_WILSON,
            "expansion_reversal_min_precision": settings._ADAPTIVE_EXPANSION_REVERSAL_MIN_PRECISION,
            "expansion_continuation_min_precision": settings._ADAPTIVE_EXPANSION_CONTINUATION_MIN_PRECISION,
            "opportunity_continuation_min_precision": settings._ADAPTIVE_OPPORTUNITY_CONTINUATION_MIN_PRECISION,
            "validation_lookback_rows": settings._ADAPTIVE_VALIDATION_LOOKBACK_ROWS,
        },
        "confidence_score": confidence_score,
        "threshold": raw["required_confidence"],
        "no_prediction_reason": None,
        "reversal_veto_reason": None,
        "analog_evidence": {
            "status": "ready",
            "direction": direction,
            "posterior_probability": signal["posterior_probability"],
            "neighbor_count": raw["neighbor_count"],
            "precision": signal["precision"],
            "lower_bound": signal["wilson_lower_bound"],
            "analog_agreement": round(raw["analog_agreement"], 6),
        },
        "analog_override": False,
        "deployment_quality_gate": None,
        "blocked_prediction": None,
        "playbook": signal,
        "adaptive_candidates": [
            {
                "profile_id": item["profile"]["id"],
                "status": item["status"],
                "direction": item.get("direction") or item.get("raw", {}).get("direction"),
                "tier": item.get("tier") or item["profile"].get("tier"),
                "precision": features._value_for_payload(item.get("precision")),
                "validation_count": item.get("validation_count", 0),
                "confidence": features._value_for_payload(item.get("raw", {}).get("confidence")),
                "blocked_reason": item.get("blocked_reason"),
            }
            for item in sorted(scored, key=lambda candidate: candidate.get("score", 0.0), reverse=True)[:5]
        ],
        "contributions": [
            {
                "horizon": f"{horizon}d",
                "feature": item["feature"],
                "value": item["target_value"],
                "impact": direction,
                "contribution": item["strength"],
            }
            for item in selected_features
        ],
        "model": model,
    }

__all__ = [name for name in globals() if not name.startswith("__")]
