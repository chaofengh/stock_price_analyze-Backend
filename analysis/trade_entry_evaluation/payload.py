from __future__ import annotations

from .settings import *
from .features import *
from .freshness import (
    build_entry_context_metadata,
    evaluate_entry_context_freshness,
)
from .decision import evaluate_row_decision
from .backtest import _empty_backtest_result, _prediction_return_values
from .quality import (
    _apply_coverage_repair_policies,
    _apply_direction_quality_gates_to_decision,
    _coverage_repair_policies_by_horizon,
    _finalize_deployment_quality,
)

def _chart_float(value: Any) -> float | None:
    num = _safe_num(value, np.nan)
    return float(num) if np.isfinite(num) else None


def _build_entry_chart_data(feature_df: pd.DataFrame) -> list[dict]:
    if feature_df.empty:
        return []
    last_date = pd.Timestamp(feature_df.iloc[-1].get("date")).normalize()
    start_date = last_date - pd.DateOffset(days=_BACKTEST_LOOKBACK_DAYS)
    chart_df = feature_df[pd.to_datetime(feature_df["date"]) >= start_date]
    out: list[dict] = []
    for _, row in chart_df.iterrows():
        out.append(
            {
                "date": _to_date_string(row.get("date")),
                "open": _chart_float(row.get("open")),
                "high": _chart_float(row.get("high")),
                "low": _chart_float(row.get("low")),
                "close": _chart_float(row.get("close")),
                "upper": _chart_float(row.get("BB_upper")),
                "lower": _chart_float(row.get("BB_lower")),
                "isTouch": row.get("touched_side") in ("Upper", "Lower"),
            }
        )
    return out


def _feature_frame_through_index(feature_df: pd.DataFrame, resolved_idx: int) -> pd.DataFrame:
    scoped = feature_df.iloc[: resolved_idx + 1].copy().reset_index(drop=True)
    attrs = dict(feature_df.attrs)
    attrs.pop("_adaptive_analog_state", None)
    scoped.attrs = attrs
    return scoped


def _ensure_decision_for_index(
    feature_df: pd.DataFrame,
    decisions_by_index: dict[int, dict],
    backtest_1y: dict,
    idx: int,
    *,
    force_prediction: bool = False,
) -> dict:
    if idx not in decisions_by_index:
        decisions_by_index[idx] = evaluate_row_decision(
            feature_df.iloc[idx],
            feature_df=feature_df,
            row_index=idx,
            force_prediction=force_prediction,
        )
        direction_gates = {
            horizon_key: result.get("direction_quality_gate", {})
            for horizon_key, result in backtest_1y.items()
            if isinstance(result, dict)
        }
        _apply_direction_quality_gates_to_decision(decisions_by_index[idx], direction_gates)
        _apply_coverage_repair_policies(
            feature_df,
            {idx: decisions_by_index[idx]},
            _coverage_repair_policies_by_horizon(backtest_1y),
        )
    return decisions_by_index[idx]


def _finalized_point_in_time_context(
    feature_df: pd.DataFrame,
    *,
    force_latest_prediction: bool = False,
) -> tuple[dict[int, dict], dict]:
    decisions_by_index: dict[int, dict] = {}
    decisions_by_index, backtest_1y = _finalize_deployment_quality(feature_df, decisions_by_index)
    latest_idx = len(feature_df) - 1
    _ensure_decision_for_index(
        feature_df,
        decisions_by_index,
        backtest_1y,
        latest_idx,
        force_prediction=force_latest_prediction,
    )
    return decisions_by_index, backtest_1y


def _point_in_time_context_for_index(
    feature_df: pd.DataFrame,
    idx: int,
    *,
    point_in_time_cache: dict[int, dict] | None = None,
) -> tuple[pd.DataFrame, dict[int, dict], dict]:
    scoped = _feature_frame_through_index(feature_df, idx)
    cached = point_in_time_cache.get(idx) if point_in_time_cache is not None else None
    if isinstance(cached, dict):
        cached_decisions = cached.get("decisions_by_index")
        cached_backtest = cached.get("backtest_1y")
        if isinstance(cached_decisions, dict) and isinstance(cached_backtest, dict):
            return scoped, deepcopy(cached_decisions), deepcopy(cached_backtest)

    decisions_by_index, backtest_1y = _finalized_point_in_time_context(scoped)
    if point_in_time_cache is not None:
        point_in_time_cache[idx] = {
            "decisions_by_index": deepcopy(decisions_by_index),
            "backtest_1y": deepcopy(backtest_1y),
        }
    return scoped, decisions_by_index, backtest_1y


def _point_in_time_decision_for_index(
    feature_df: pd.DataFrame,
    idx: int,
    *,
    cache: dict[int, dict] | None = None,
    point_in_time_cache: dict[int, dict] | None = None,
) -> dict:
    if cache is not None and idx in cache:
        return cache[idx]

    scoped, decisions_by_index, _backtest_1y = _point_in_time_context_for_index(
        feature_df,
        idx,
        point_in_time_cache=point_in_time_cache,
    )
    decision = deepcopy(decisions_by_index[len(scoped) - 1])
    if cache is not None:
        cache[idx] = decision
    return decision


def _open_prediction_row(
    feature_df: pd.DataFrame,
    idx: int,
    horizon: int,
    horizon_decision: dict,
) -> dict:
    row = feature_df.iloc[idx]
    current_row = feature_df.iloc[-1]
    signal_close = _safe_num(row.get("close"), np.nan)
    current_close = _safe_num(current_row.get("close"), np.nan)
    atr = _safe_num(row.get("ATR14"), np.nan)
    elapsed_sessions = max(0, len(feature_df) - 1 - idx)
    remaining_sessions = max(0, horizon - elapsed_sessions)
    progress = _clamp(elapsed_sessions / horizon, 0.0, 1.0) if horizon > 0 else 0.0
    flat_tolerance = _flat_tolerance_for_row(row)
    interim_direction = (
        _actual_direction(row.get("touched_side"), signal_close, current_close, flat_tolerance)
        if np.isfinite(signal_close) and np.isfinite(current_close)
        else None
    )
    predicted_direction = horizon_decision.get("predicted_direction")
    if interim_direction == "flat":
        interim_status = "flat"
    elif interim_direction in ("continuation", "reversal") and predicted_direction == interim_direction:
        interim_status = "working"
    elif interim_direction in ("continuation", "reversal"):
        interim_status = "against"
    else:
        interim_status = "unknown"
    current_return_values = _prediction_return_values(
        row.get("touched_side"),
        predicted_direction,
        signal_close,
        current_close,
        atr,
    )
    playbook = horizon_decision.get("playbook") or {}
    return {
        "status": "open",
        "signal_date": _to_date_string(row.get("date")),
        "current_date": _to_date_string(current_row.get("date")),
        "outcome_date": None,
        "horizon_days": horizon,
        "elapsed_sessions": elapsed_sessions,
        "remaining_sessions": remaining_sessions,
        "progress": round(float(progress), 6),
        "touched_side": row.get("touched_side"),
        "predicted_direction": predicted_direction,
        "interim_direction": interim_direction,
        "interim_status": interim_status,
        "actual_direction": None,
        "signal_close": round(signal_close, 6) if np.isfinite(signal_close) else None,
        "current_close": round(current_close, 6) if np.isfinite(current_close) else None,
        "outcome_close": None,
        "continuation_hurdle": round(_continuation_hurdle_for_row(row), 6),
        "flat_tolerance": round(flat_tolerance, 6),
        "is_correct": None,
        "trade_direction": _prediction_return_values(
            row.get("touched_side"),
            predicted_direction,
            signal_close,
            signal_close,
            atr,
        ).get("trade_direction"),
        "current_trade_return": current_return_values.get("trade_return"),
        "current_trade_return_atr": current_return_values.get("trade_return_atr"),
        "trade_return": None,
        "trade_return_atr": None,
        "continuation_probability": horizon_decision.get("continuation_probability"),
        "reversal_probability": horizon_decision.get("reversal_probability"),
        "confidence_score": horizon_decision.get("confidence_score"),
        "signal_model": playbook.get("name"),
        "signal_model_id": playbook.get("id") or (playbook.get("profile") or {}).get("id"),
        "signal_precision": playbook.get("precision"),
        "signal_tier": playbook.get("tier") or (playbook.get("profile") or {}).get("tier"),
    }


def _build_open_predictions_by_horizon(
    feature_df: pd.DataFrame,
    *,
    decisions_by_index: dict[int, dict],
    backtest_1y: dict,
    selected_idx: int,
    selected_decision: dict,
) -> dict[str, list[dict]]:
    open_by_horizon = {f"{horizon}d": [] for horizon in _HORIZONS}
    if feature_df.empty:
        return open_by_horizon

    last_date = pd.Timestamp(feature_df.iloc[-1].get("date")).normalize()
    period_start = max(
        pd.Timestamp(feature_df.iloc[0].get("date")).normalize(),
        last_date - pd.DateOffset(days=_BACKTEST_LOOKBACK_DAYS),
    )
    first_open_idx = max(0, len(feature_df) - max(_HORIZONS))

    for idx in range(first_open_idx, len(feature_df)):
        row = feature_df.iloc[idx]
        signal_date = pd.Timestamp(row.get("date")).normalize()
        if signal_date < period_start:
            continue
        if _safe_bool(row.get("event_risk_blocked")):
            continue
        if row.get("touched_side") not in ("Upper", "Lower"):
            continue

        decision = (
            selected_decision
            if idx == selected_idx
            else _ensure_decision_for_index(
                feature_df,
                decisions_by_index,
                backtest_1y,
                idx,
            )
        )
        for horizon in _HORIZONS:
            if idx + horizon < len(feature_df):
                continue
            horizon_key = f"{horizon}d"
            horizon_decision = decision.get("horizons", {}).get(horizon_key) or {}
            if horizon_decision.get("status") != "prediction":
                continue
            open_by_horizon[horizon_key].append(
                _open_prediction_row(feature_df, idx, horizon, horizon_decision)
            )

    return open_by_horizon


def _attach_open_predictions_to_backtest(
    backtest_1y: dict,
    feature_df: pd.DataFrame,
    *,
    decisions_by_index: dict[int, dict],
    selected_idx: int,
    selected_decision: dict,
) -> dict:
    out = deepcopy(backtest_1y)
    open_by_horizon = _build_open_predictions_by_horizon(
        feature_df,
        decisions_by_index=decisions_by_index,
        backtest_1y=out,
        selected_idx=selected_idx,
        selected_decision=selected_decision,
    )
    for horizon in _HORIZONS:
        horizon_key = f"{horizon}d"
        result = out.get(horizon_key)
        if not isinstance(result, dict):
            result = _empty_backtest_result(feature_df, horizon)
            out[horizon_key] = result
        result["open_predictions"] = open_by_horizon.get(horizon_key, [])
        result["open_prediction_count"] = len(result["open_predictions"])
    return out


def build_entry_decision_from_frame(
    symbol: str,
    frame: pd.DataFrame,
    *,
    as_of_date: str | pd.Timestamp | None = None,
    earnings_dates: set[pd.Timestamp] | None = None,
    earnings_symbol: str | None = None,
    context_frames: dict[str, pd.DataFrame] | None = None,
) -> dict:
    context = build_entry_decision_context_from_frame(
        symbol,
        frame,
        earnings_dates=earnings_dates,
        earnings_symbol=earnings_symbol,
        context_frames=context_frames,
    )
    return build_entry_decision_from_context(context, as_of_date=as_of_date)


def build_entry_decision_context_from_frame(
    symbol: str,
    frame: pd.DataFrame,
    *,
    earnings_dates: set[pd.Timestamp] | None = None,
    earnings_symbol: str | None = None,
    context_frames: dict[str, pd.DataFrame] | None = None,
) -> dict:
    normalized = normalize_symbol(symbol)
    if not normalized:
        raise ValueError("Missing symbol for entry decision")
    if frame is None or frame.empty:
        raise ValueError(f"No data found for symbol {normalized}")

    feature_df = _prepare_feature_frame(
        frame,
        symbol=normalize_symbol(earnings_symbol or normalized),
        earnings_dates=earnings_dates,
        context_frames=context_frames,
    )
    decisions_by_index: dict[int, dict] = {}
    decisions_by_index, backtest_1y = _finalize_deployment_quality(feature_df, decisions_by_index)
    return {
        "symbol": normalized,
        "feature_df": feature_df,
        "decisions_by_index": decisions_by_index,
        "backtest_1y": backtest_1y,
        "_point_in_time_cache": {},
        "meta": build_entry_context_metadata(normalized, feature_df, backtest_1y),
    }


def build_entry_decision_from_context(
    context: dict,
    *,
    as_of_date: str | pd.Timestamp | None = None,
) -> dict:
    if not isinstance(context, dict):
        raise ValueError("Entry decision context is unavailable.")
    symbol = normalize_symbol(context.get("symbol"))
    feature_df = context.get("feature_df")
    decisions_by_index = context.get("decisions_by_index")
    backtest_1y = context.get("backtest_1y")
    point_in_time_cache = context.get("_point_in_time_cache")
    context_meta = context.get("meta")
    if not symbol:
        raise ValueError("Entry decision context is missing a symbol.")
    if feature_df is None or getattr(feature_df, "empty", True):
        raise ValueError(f"Entry decision context for {symbol} has no feature data.")
    if not isinstance(decisions_by_index, dict):
        decisions_by_index = {}
    if not isinstance(backtest_1y, dict):
        backtest_1y = {}
    if not isinstance(point_in_time_cache, dict):
        point_in_time_cache = {}
        context["_point_in_time_cache"] = point_in_time_cache
    return _build_payload_from_context(
        symbol,
        as_of_date=as_of_date,
        feature_df=feature_df,
        decisions_by_index=decisions_by_index,
        backtest_1y=backtest_1y,
        point_in_time_cache=point_in_time_cache,
        context_meta=context_meta,
    )


def _load_entry_frame(symbol: str) -> tuple[str, pd.DataFrame]:
    for candidate in symbol_candidates(symbol):
        data_dict = prepare_stock_data(
            candidate,
            include_rsi=False,
            period=_TRAINING_HISTORY_PERIOD,
            interval="1d",
        )
        df = data_dict.get(candidate)
        if df is None or df.empty:
            continue
        if "close" not in df.columns:
            continue
        return candidate, df
    return "", pd.DataFrame()


def _load_context_frames(symbol: str) -> dict[str, pd.DataFrame]:
    context_frames: dict[str, pd.DataFrame] = {}
    for context_symbol in ("QQQ", "XLK"):
        try:
            data_dict = prepare_stock_data(
                context_symbol,
                include_rsi=False,
                period=_TRAINING_HISTORY_PERIOD,
                interval="1d",
            )
        except Exception:
            continue
        df = data_dict.get(context_symbol)
        if df is None or df.empty:
            continue
        context_frames[context_symbol] = df
    return context_frames


def _entry_cache_day() -> str:
    return pd.Timestamp.now(tz="America/Chicago").strftime("%Y-%m-%d")


@lru_cache(maxsize=_ENTRY_CONTEXT_CACHE_SIZE)
def _get_entry_feature_context_cached(symbol: str, cache_day: str) -> tuple[str, pd.DataFrame, dict]:
    _ = cache_day
    resolved_symbol, frame = _load_entry_frame(symbol)
    if frame.empty:
        raise ValueError(f"No data found for symbol {symbol}")

    feature_df = _prepare_feature_frame(
        frame,
        symbol=normalize_symbol(resolved_symbol or symbol),
        earnings_dates=None,
        context_frames=_load_context_frames(resolved_symbol or symbol),
    )
    return resolved_symbol or symbol, feature_df, {}


@lru_cache(maxsize=_ENTRY_CONTEXT_CACHE_SIZE)
def _get_entry_context_cached(symbol: str, cache_day: str) -> tuple[str, pd.DataFrame, dict[int, dict], dict, dict, dict]:
    resolved_symbol, feature_df, point_in_time_cache = _get_entry_feature_context_cached(
        symbol,
        cache_day,
    )
    decisions_by_index, backtest_1y = _finalized_point_in_time_context(
        feature_df,
        force_latest_prediction=True,
    )
    context_meta = build_entry_context_metadata(symbol, feature_df, backtest_1y)
    return resolved_symbol or symbol, feature_df, decisions_by_index, backtest_1y, point_in_time_cache, context_meta


def _build_payload_from_context(
    symbol: str,
    *,
    as_of_date: str | pd.Timestamp | None,
    feature_df: pd.DataFrame,
    decisions_by_index: dict[int, dict],
    backtest_1y: dict,
    point_in_time_cache: dict[int, dict] | None = None,
    context_meta: dict | None = None,
) -> dict:
    parsed_as_of_date = _parse_as_of_date(as_of_date)
    resolved_idx, date_was_snapped = _resolve_as_of_index(feature_df, parsed_as_of_date)
    latest_selected = resolved_idx == len(feature_df) - 1
    scoped_feature_df = feature_df if latest_selected else _feature_frame_through_index(feature_df, resolved_idx)
    selected_idx = resolved_idx

    selected_row = feature_df.iloc[selected_idx]
    selected_decision = deepcopy(
        _ensure_decision_for_index(
            feature_df,
            decisions_by_index,
            backtest_1y,
            selected_idx,
            force_prediction=latest_selected,
        )
    )
    scoped_backtest_1y = _attach_open_predictions_to_backtest(
        backtest_1y,
        scoped_feature_df,
        decisions_by_index=decisions_by_index,
        selected_idx=selected_idx,
        selected_decision=selected_decision,
    )

    context_meta = context_meta or build_entry_context_metadata(symbol, feature_df, backtest_1y)
    return {
        "symbol": symbol,
        "requested_as_of_date": _to_date_string(parsed_as_of_date),
        "as_of_date": _to_date_string(selected_row.get("date")),
        "date_was_snapped": bool(parsed_as_of_date is not None and date_was_snapped),
        "touched_side": selected_decision["touched_side"],
        "setup_type": selected_decision["setup_type"],
        "event_risk_blocked": selected_decision.get("event_risk_blocked", False),
        "event_risk": deepcopy(selected_decision.get("event_risk") or {}),
        "prediction_threshold": _PREDICTION_THRESHOLD,
        "deployment_thresholds": {
            "continuation": _CONTINUATION_DEPLOYMENT_THRESHOLD,
            "reversal": _REVERSAL_DEPLOYMENT_THRESHOLD,
        },
        "context_status": deepcopy(scoped_feature_df.attrs.get("context_status", {})),
        "horizons": selected_decision["horizons"],
        "top_reasons": selected_decision["top_reasons"],
        "backtest_1y": scoped_backtest_1y,
        "chart_data": _build_entry_chart_data(scoped_feature_df),
        "meta": {
            "full_decision_preloaded": True,
            "context": deepcopy(context_meta),
            "freshness": evaluate_entry_context_freshness(context_meta),
            "quality": deepcopy(context_meta.get("quality", {})),
        },
    }


def get_entry_decision(symbol: str, as_of_date: str | None = None) -> dict:
    normalized = normalize_symbol(symbol)
    if not normalized:
        raise ValueError("Missing symbol for entry decision")

    context = get_entry_decision_context(normalized)
    return build_entry_decision_from_context(context, as_of_date=as_of_date)


def get_entry_decision_context(symbol: str) -> dict:
    normalized = normalize_symbol(symbol)
    if not normalized:
        raise ValueError("Missing symbol for entry decision")

    _, feature_df, decisions_by_index, backtest_1y, point_in_time_cache, context_meta = _get_entry_context_cached(
        normalized,
        _entry_cache_day(),
    )
    return {
        "symbol": normalized,
        "feature_df": feature_df,
        "decisions_by_index": decisions_by_index,
        "backtest_1y": backtest_1y,
        "_point_in_time_cache": point_in_time_cache,
        "meta": context_meta,
    }




__all__ = [name for name in globals() if not name.startswith("__")]
