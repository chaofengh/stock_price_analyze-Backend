from __future__ import annotations

from bisect import bisect_right

import numpy as np
import pandas as pd

from . import features, settings
from .playbooks import _actual_direction_for_index


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
    trend = _empirical_bucket(
        trend_value,
        (-0.025, 0.015, 0.055, 0.105) if horizon >= 10 else (-0.015, 0.010, 0.035, 0.075),
    )
    market_value = (side_qqq + side_xlk) / 2.0
    band_state = (
        "wide_expanding"
        if band_rank >= 0.75 and bandwidth_change >= 0.02
        else "wide_stalling"
        if band_rank >= 0.75
        else "compressed"
        if band_rank <= 0.30
        else "normal_band"
    )
    return {
        "side": str(side),
        "touch_quality": touch_quality,
        "cluster": cluster,
        "trend": trend,
        "longer_trend": _empirical_bucket(side_ret_20d, (-0.040, 0.010, 0.070, 0.145)),
        "market": _empirical_bucket(market_value, (-0.020, -0.004, 0.008, 0.025)),
        "volume_pressure": _empirical_bucket(pressure, (-0.25, -0.05, 0.12, 0.32)),
        "participation": "high_volume" if rel_volume >= 1.25 else "dry_volume" if rel_volume <= 0.80 else "normal_volume",
        "band_state": band_state,
        "adx_state": "strong_trend" if adx >= 28 else "weak_trend" if adx <= 18 else "moderate_trend",
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


def _empirical_regime_index(feature_df: pd.DataFrame, horizon: int) -> dict:
    cache_root = feature_df.attrs.get("_empirical_regime_index")
    if not isinstance(cache_root, settings._NoDeepcopyDict):
        cache_root = settings._NoDeepcopyDict(cache_root or {})
        feature_df.attrs["_empirical_regime_index"] = cache_root
    if horizon in cache_root:
        return cache_root[horizon]

    by_scope: dict[tuple[tuple[str, ...], tuple[str | None, ...]], dict[str, list]] = {}
    for idx in range(max(0, len(feature_df) - horizon)):
        row = feature_df.iloc[idx]
        if features._safe_bool(row.get("event_risk_blocked")) or row.get("touched_side") not in ("Upper", "Lower"):
            continue
        actual = _actual_direction_for_index(feature_df, idx, horizon)
        if actual not in ("continuation", "reversal", "flat"):
            continue
        outcome_row = feature_df.iloc[idx + horizon]
        signal_close = features._safe_num(row.get("close"), np.nan)
        outcome_close = features._safe_num(outcome_row.get("close"), np.nan)
        continuation_target_hit = features._direction_target_hit_for_index(
            feature_df,
            idx,
            horizon,
            "continuation",
        )
        reversal_target_hit = features._direction_target_hit_for_index(
            feature_df,
            idx,
            horizon,
            "reversal",
        )
        attrs = _empirical_regime_attrs(row, horizon)
        public_row = {
            "idx": idx,
            "signal_date": features._to_date_string(row.get("date")),
            "outcome_date": features._to_date_string(outcome_row.get("date")),
            "direction": actual,
            "actual_direction": actual,
            "touched_side": row.get("touched_side"),
            "signal_close": round(signal_close, 6) if np.isfinite(signal_close) else None,
            "outcome_close": round(outcome_close, 6) if np.isfinite(outcome_close) else None,
            "horizon_days": int(horizon),
            "target_atr_multiple": settings._DIRECTION_ATR_THRESHOLD,
            "continuation_target_hit": continuation_target_hit,
            "reversal_target_hit": reversal_target_hit,
        }
        for _, fields, _ in _empirical_regime_specs(horizon):
            key = (fields, tuple(attrs.get(field) for field in fields))
            bucket = by_scope.setdefault(key, {"indices": [], "rows": []})
            bucket["indices"].append(idx)
            bucket["rows"].append(public_row)

    cache_root[horizon] = {"by_scope": by_scope}
    return cache_root[horizon]


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
    key = (fields, tuple(target_attrs.get(field) for field in fields))
    bucket = (_empirical_regime_index(feature_df, horizon).get("by_scope") or {}).get(key)
    if not bucket:
        return []
    cutoff = bisect_right(bucket["indices"], max_idx)
    start = max(0, cutoff - settings._EMPIRICAL_REGIME_MAX_MATCHES)
    return bucket["rows"][start:cutoff] if cutoff > 0 else []


__all__ = [name for name in globals() if not name.startswith("__")]
