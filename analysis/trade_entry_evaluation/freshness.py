from __future__ import annotations

import hashlib
import json
import os
from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import Any

import pandas_market_calendars as mcal
import pytz

from .settings import *
from .features import _safe_num, _to_date_string

_CHICAGO_TZ = pytz.timezone("America/Chicago")
_NYSE = mcal.get_calendar("NYSE")
_MAX_LOOKBACK_DAYS = 21


def _stable_digest(payload: dict) -> str:
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:12]


@lru_cache(maxsize=1)
def entry_decision_feature_schema_version() -> str:
    return f"features-{_stable_digest({'features': _MODEL_FEATURES})}"


@lru_cache(maxsize=1)
def entry_decision_model_version() -> str:
    payload = {
        "horizons": _HORIZONS,
        "model_features": _MODEL_FEATURES,
        "analog_features": _ANALOG_FEATURES,
        "prediction_threshold": _PREDICTION_THRESHOLD,
        "continuation_deployment_threshold": _CONTINUATION_DEPLOYMENT_THRESHOLD,
        "reversal_deployment_threshold": _REVERSAL_DEPLOYMENT_THRESHOLD,
        "flat_price_absolute_tolerance": _FLAT_PRICE_ABSOLUTE_TOLERANCE,
        "flat_tolerance_atr_multiple": _DIRECTION_ATR_THRESHOLD,
        "flat_reversal_predictions_count_as_correct": False,
        "label_sampling_policy": "bollinger_touch_events_only",
        "earnings_blackout_pre_event_sessions": _EARNINGS_BLACKOUT_PRE_EVENT_SESSIONS,
        "earnings_blackout_post_event_sessions": _EARNINGS_BLACKOUT_POST_EVENT_SESSIONS,
        "earnings_date_lookahead_calendar_days": _EARNINGS_DATE_LOOKAHEAD_CALENDAR_DAYS,
        "coverage_expansion_max_accuracy_drop": _COVERAGE_EXPANSION_MAX_ACCURACY_DROP,
        "coverage_repair_policy_version": 2,
        "coverage_repair_calibration": "posterior_precision_wilson_v1",
        "empirical_regime_label_policy": "include_flat_outcomes_in_precision_denominator",
        "adaptive_veto_policy_version": 3,
        "empirical_reversal_min_precision": _EMPIRICAL_REVERSAL_MIN_PRECISION,
        "empirical_continuation_min_precision": _EMPIRICAL_CONTINUATION_MIN_PRECISION,
        "empirical_reversal_min_wilson": _EMPIRICAL_REVERSAL_MIN_WILSON,
        "empirical_continuation_min_wilson": _EMPIRICAL_CONTINUATION_MIN_WILSON,
        "empirical_reversal_min_matches": _EMPIRICAL_REVERSAL_MIN_MATCHES,
        "empirical_continuation_min_matches": _EMPIRICAL_CONTINUATION_MIN_MATCHES,
        "empirical_reversal_min_recent_precision": _EMPIRICAL_REVERSAL_MIN_RECENT_PRECISION,
        "empirical_continuation_min_recent_precision": _EMPIRICAL_CONTINUATION_MIN_RECENT_PRECISION,
        "empirical_recent_reversal_min_full_precision": _EMPIRICAL_RECENT_REVERSAL_MIN_FULL_PRECISION,
        "empirical_recent_continuation_min_full_precision": _EMPIRICAL_RECENT_CONTINUATION_MIN_FULL_PRECISION,
        "production_backtest_policy": _BACKTEST_REPORTING_POLICY,
        "deployment_gate_policy_version": 8,
        "deployment_evidence_lookback_days": _DEPLOYMENT_EVIDENCE_LOOKBACK_DAYS,
        "deployment_wilson_z_value": _DEPLOYMENT_WILSON_Z,
        "deployment_min_backtest_calls": _DEPLOY_MIN_BACKTEST_CALLS,
        "deployment_min_signal_backtest_calls": _DEPLOY_MIN_SIGNAL_BACKTEST_CALLS,
        "deployment_min_backtest_accuracy": _DEPLOY_MIN_BACKTEST_ACCURACY,
        "deployment_min_backtest_reverse_accuracy": _DEPLOY_MIN_BACKTEST_REVERSE_ACCURACY,
        "deployment_min_backtest_continue_accuracy": _DEPLOY_MIN_BACKTEST_CONTINUE_ACCURACY,
        "deployment_min_backtest_wilson": _DEPLOY_MIN_BACKTEST_WILSON,
        "deployment_min_backtest_reverse_wilson": _DEPLOY_MIN_BACKTEST_REVERSE_WILSON,
        "deployment_min_backtest_continue_wilson": _DEPLOY_MIN_BACKTEST_CONTINUE_WILSON,
        "deployment_min_signal_accuracy": _DEPLOY_MIN_SIGNAL_BACKTEST_ACCURACY,
        "deployment_min_signal_reverse_wilson": _DEPLOY_MIN_SIGNAL_BACKTEST_REVERSE_WILSON,
        "deployment_min_signal_continue_wilson": _DEPLOY_MIN_SIGNAL_BACKTEST_CONTINUE_WILSON,
        "deployment_min_fallback_signal_backtest_calls": _DEPLOY_MIN_FALLBACK_SIGNAL_BACKTEST_CALLS,
        "deployment_min_fallback_signal_accuracy": _DEPLOY_MIN_FALLBACK_SIGNAL_ACCURACY,
        "deployment_min_fallback_signal_reverse_wilson": _DEPLOY_MIN_FALLBACK_SIGNAL_REVERSE_WILSON,
        "deployment_min_fallback_signal_continue_wilson": _DEPLOY_MIN_FALLBACK_SIGNAL_CONTINUE_WILSON,
        "deployment_min_broad_family_backtest_calls": _DEPLOY_MIN_BROAD_FAMILY_BACKTEST_CALLS,
        "deployment_min_broad_family_accuracy": _DEPLOY_MIN_BROAD_FAMILY_ACCURACY,
        "deployment_min_broad_family_wilson": _DEPLOY_MIN_BROAD_FAMILY_WILSON,
        "deployment_min_signal_expected_return": _DEPLOY_MIN_SIGNAL_EXPECTED_RETURN,
        "latest_prediction_scope": "bollinger_touch_days",
        "training_history_period": _TRAINING_HISTORY_PERIOD,
        "model_iterations": _MODEL_ITERATIONS,
        "model_learning_rate": _MODEL_LEARNING_RATE,
        "model_l2": _MODEL_L2,
        "logistic_lookbacks": _LOGISTIC_LOOKBACKS,
        "rbf_specs": _RBF_SPECS,
        "adaptive_profiles": _ADAPTIVE_ANALOG_PROFILES,
    }
    return f"entry-{_stable_digest(payload)}"


def _now_chi() -> datetime:
    return datetime.now(_CHICAGO_TZ)


def _as_chicago_time(value: datetime | None = None) -> datetime:
    if value is None:
        return _now_chi()
    if value.tzinfo is None:
        return _CHICAGO_TZ.localize(value)
    return value.astimezone(_CHICAGO_TZ)


def _normalize_price_date(value: Any) -> str | None:
    text = _to_date_string(value)
    if not text:
        return None
    try:
        date.fromisoformat(text)
    except (TypeError, ValueError):
        return None
    return text


def _availability_lag() -> timedelta:
    raw = os.getenv("ENTRY_DECISION_DATA_AFTER_CLOSE_LAG_MINUTES")
    if raw is None:
        minutes = 30.0
    else:
        try:
            minutes = float(raw)
        except ValueError:
            minutes = 30.0
    return timedelta(minutes=max(0.0, minutes))


def _max_stale_sessions() -> int:
    raw = os.getenv("ENTRY_DECISION_CONTEXT_MAX_STALE_SESSIONS")
    if raw is None:
        return 1
    try:
        return max(0, int(raw))
    except ValueError:
        return 1


@lru_cache(maxsize=64)
def _session_dates_with_ready_close(end_day: str, ready_before_iso: str) -> tuple[str, ...]:
    end = date.fromisoformat(end_day)
    start = end - timedelta(days=_MAX_LOOKBACK_DAYS)
    ready_before = datetime.fromisoformat(ready_before_iso)
    schedule = _NYSE.schedule(start_date=start.isoformat(), end_date=end.isoformat())
    if schedule.empty:
        return ()

    ready_dates: list[str] = []
    for session_day, session in schedule.iterrows():
        market_close = session.get("market_close")
        if market_close is None:
            continue
        close_chi = market_close.tz_convert(_CHICAGO_TZ).to_pydatetime()
        if close_chi <= ready_before:
            ready_dates.append(session_day.date().isoformat())
    return tuple(ready_dates)


def latest_required_price_date(now: datetime | None = None) -> str | None:
    now_chi = _as_chicago_time(now)
    ready_before = now_chi - _availability_lag()
    ready_dates = _session_dates_with_ready_close(
        now_chi.date().isoformat(),
        ready_before.replace(second=0, microsecond=0).isoformat(),
    )
    if not ready_dates:
        return None
    return ready_dates[-1]


@lru_cache(maxsize=128)
def _trading_sessions_between(start_date: str, end_date: str) -> int:
    if start_date >= end_date:
        return 0
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    schedule = _NYSE.schedule(start_date=start.isoformat(), end_date=end.isoformat())
    if schedule.empty:
        return 0
    return sum(1 for session_day in schedule.index if start_date < session_day.date().isoformat() <= end_date)


def _context_quality_summary(backtest_1y: dict) -> dict:
    horizons: dict[str, dict] = {}
    passed = 0
    quarantined = 0
    idle = 0
    for horizon_key in (f"{horizon}d" for horizon in _HORIZONS):
        result = backtest_1y.get(horizon_key)
        if not isinstance(result, dict):
            continue
        gate = result.get("quality_gate") if isinstance(result.get("quality_gate"), dict) else {}
        status = gate.get("status") or "unknown"
        if status == "passed":
            passed += 1
        elif status == "quarantined":
            quarantined += 1
        elif status == "idle":
            idle += 1
        horizons[horizon_key] = {
            "status": status,
            "accuracy": result.get("accuracy"),
            "reversal_accuracy": result.get("reversal_accuracy"),
            "continuation_accuracy": result.get("continuation_accuracy"),
            "coverage": result.get("coverage"),
            "prediction_count": int(_safe_num(result.get("prediction_count"), 0)),
            "reversal_call_count": int(_safe_num(result.get("reversal_call_count"), 0)),
            "continuation_call_count": int(_safe_num(result.get("continuation_call_count"), 0)),
        }

    if passed:
        status = "passed"
    elif quarantined:
        status = "quarantined"
    elif idle:
        status = "idle"
    else:
        status = "unknown"
    return {
        "status": status,
        "passed_horizon_count": passed,
        "quarantined_horizon_count": quarantined,
        "idle_horizon_count": idle,
        "horizons": horizons,
    }


def build_entry_context_metadata(
    symbol: str,
    feature_df,
    backtest_1y: dict,
    *,
    created_at: datetime | None = None,
) -> dict:
    created = _as_chicago_time(created_at)
    if feature_df is None or getattr(feature_df, "empty", True) or "date" not in feature_df.columns:
        price_start = None
        price_end = None
        row_count = 0
    else:
        dates = feature_df["date"]
        price_start = _to_date_string(dates.iloc[0])
        price_end = _to_date_string(dates.iloc[-1])
        row_count = int(len(feature_df))

    model_version = entry_decision_model_version()
    feature_schema_version = entry_decision_feature_schema_version()
    quality = _context_quality_summary(backtest_1y)
    base = {
        "symbol": str(symbol or "").upper(),
        "model_version": model_version,
        "feature_schema_version": feature_schema_version,
        "price_data_start_date": price_start,
        "price_data_end_date": price_end,
        "trained_through_date": price_end,
        "created_at": created.strftime("%Y-%m-%d %H:%M:%S"),
        "created_timezone": "America/Chicago",
        "training_history_period": _TRAINING_HISTORY_PERIOD,
        "feature_row_count": row_count,
        "quality": quality,
    }
    base["context_key"] = _stable_digest(
        {
            "symbol": base["symbol"],
            "model_version": model_version,
            "feature_schema_version": feature_schema_version,
            "price_data_end_date": price_end,
            "feature_row_count": row_count,
        }
    )
    return base


def evaluate_entry_context_freshness(
    metadata: dict | None,
    *,
    now: datetime | None = None,
) -> dict:
    if not isinstance(metadata, dict) or not metadata:
        return {
            "status": "unknown",
            "serving_allowed": True,
            "reason": "missing_metadata",
            "latest_required_price_date": latest_required_price_date(now),
        }

    expected_model = entry_decision_model_version()
    expected_schema = entry_decision_feature_schema_version()
    model_version = metadata.get("model_version")
    schema_version = metadata.get("feature_schema_version")
    price_end = _normalize_price_date(metadata.get("price_data_end_date"))
    required_date = latest_required_price_date(now)

    if model_version != expected_model:
        return {
            "status": "expired",
            "serving_allowed": False,
            "reason": "model_version_changed",
            "model_version": model_version,
            "expected_model_version": expected_model,
            "latest_required_price_date": required_date,
            "price_data_end_date": price_end,
        }
    if schema_version != expected_schema:
        return {
            "status": "expired",
            "serving_allowed": False,
            "reason": "feature_schema_changed",
            "feature_schema_version": schema_version,
            "expected_feature_schema_version": expected_schema,
            "latest_required_price_date": required_date,
            "price_data_end_date": price_end,
        }
    if not price_end or required_date is None:
        return {
            "status": "unknown",
            "serving_allowed": True,
            "reason": "missing_price_date" if not price_end else "calendar_unavailable",
            "latest_required_price_date": required_date,
            "price_data_end_date": price_end,
        }
    if price_end >= required_date:
        return {
            "status": "fresh",
            "serving_allowed": True,
            "reason": "latest_required_data_available",
            "latest_required_price_date": required_date,
            "price_data_end_date": price_end,
            "stale_sessions": 0,
        }

    stale_sessions = _trading_sessions_between(price_end, required_date)
    max_stale = _max_stale_sessions()
    return {
        "status": "stale" if stale_sessions <= max_stale else "expired",
        "serving_allowed": stale_sessions <= max_stale,
        "reason": "price_data_lag",
        "latest_required_price_date": required_date,
        "price_data_end_date": price_end,
        "stale_sessions": stale_sessions,
        "max_stale_sessions": max_stale,
    }


def context_metadata_from_payload(payload: dict | None) -> dict | None:
    if not isinstance(payload, dict):
        return None
    meta = payload.get("meta")
    if not isinstance(meta, dict):
        return None
    context_meta = meta.get("context")
    if isinstance(context_meta, dict):
        return context_meta
    if any(key in meta for key in ("model_version", "feature_schema_version", "price_data_end_date")):
        return meta
    return None


def refresh_payload_freshness(payload: dict, *, now: datetime | None = None) -> dict:
    if not isinstance(payload, dict):
        return payload
    meta = payload.setdefault("meta", {})
    if not isinstance(meta, dict):
        meta = {}
        payload["meta"] = meta
    context_meta = context_metadata_from_payload(payload)
    meta["freshness"] = evaluate_entry_context_freshness(context_meta, now=now)
    return payload


def entry_context_serving_allowed(context: dict | None, *, now: datetime | None = None) -> bool:
    if not isinstance(context, dict):
        return False
    freshness = evaluate_entry_context_freshness(context.get("meta"), now=now)
    return bool(freshness.get("serving_allowed"))


def entry_payload_serving_allowed(payload: dict | None, *, now: datetime | None = None) -> bool:
    if not isinstance(payload, dict):
        return False
    freshness = evaluate_entry_context_freshness(context_metadata_from_payload(payload), now=now)
    return bool(freshness.get("serving_allowed"))


__all__ = [
    "build_entry_context_metadata",
    "context_metadata_from_payload",
    "entry_context_serving_allowed",
    "entry_decision_feature_schema_version",
    "entry_decision_model_version",
    "entry_payload_serving_allowed",
    "evaluate_entry_context_freshness",
    "latest_required_price_date",
    "refresh_payload_freshness",
]
