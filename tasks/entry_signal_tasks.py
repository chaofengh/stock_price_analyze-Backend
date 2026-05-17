from __future__ import annotations

import logging
from typing import Iterable

from analysis.data_fetcher_utils import normalize_symbol
from database.entry_signal_repository import (
    get_open_entry_signal_symbols as _repo_open_symbols,
    list_open_entry_signal_keys,
    list_open_entry_signals as _repo_list_open_signals,
    upsert_entry_decision_signal,
)

logger = logging.getLogger(__name__)


def _date_text(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text[:10] if text else None


def _horizon_days_from_key(horizon_key: str, fallback=None) -> int | None:
    try:
        return int(str(horizon_key).lower().replace("d", ""))
    except (TypeError, ValueError):
        try:
            return int(fallback)
        except (TypeError, ValueError):
            return None


def _payload_context(payload: dict) -> dict:
    meta = payload.get("meta") if isinstance(payload, dict) else {}
    if not isinstance(meta, dict):
        return {}
    context = meta.get("context")
    return context if isinstance(context, dict) else {}


def _payload_is_current_for_signal_sync(payload: dict) -> bool:
    """
    Only persist signals from payloads rendered against the latest model context.

    Historical date-picker payloads intentionally show what was open on that old
    date. Persisting those would resurrect old trades as if they were open today.
    """
    if not isinstance(payload, dict):
        return False
    as_of_date = _date_text(payload.get("as_of_date"))
    if not as_of_date:
        return False
    context = _payload_context(payload)
    price_data_end_date = _date_text(context.get("price_data_end_date"))
    if not price_data_end_date:
        return False
    return as_of_date >= price_data_end_date


def _current_horizon_details(payload: dict, horizon_days: int, signal_date: str | None) -> dict:
    if signal_date != _date_text(payload.get("as_of_date")):
        return {}
    horizon = (payload.get("horizons") or {}).get(f"{horizon_days}d")
    return horizon if isinstance(horizon, dict) else {}


def _signal_base(symbol: str, payload: dict, source: str) -> dict:
    context = _payload_context(payload)
    return {
        "symbol": normalize_symbol(symbol),
        "source": source or "entry_decision",
        "model_version": context.get("model_version"),
        "feature_schema_version": context.get("feature_schema_version"),
        "payload_as_of_date": _date_text(payload.get("as_of_date")),
        "price_data_end_date": _date_text(context.get("price_data_end_date")),
    }


def _row_from_open_prediction(symbol: str, payload: dict, horizon_key: str, prediction: dict, source: str) -> dict | None:
    horizon_days = _horizon_days_from_key(horizon_key, prediction.get("horizon_days"))
    signal_date = _date_text(prediction.get("signal_date"))
    predicted_direction = prediction.get("predicted_direction")
    if not horizon_days or not signal_date or predicted_direction not in ("continuation", "reversal"):
        return None

    current_horizon = _current_horizon_details(payload, horizon_days, signal_date)
    playbook = current_horizon.get("playbook") if isinstance(current_horizon.get("playbook"), dict) else None
    return {
        **_signal_base(symbol, payload, source),
        "signal_date": signal_date,
        "horizon_days": horizon_days,
        "status": "open",
        "touched_side": prediction.get("touched_side"),
        "predicted_direction": predicted_direction,
        "trade_direction": prediction.get("trade_direction"),
        "signal_close": prediction.get("signal_close"),
        "current_date": _date_text(prediction.get("current_date")),
        "current_close": prediction.get("current_close"),
        "outcome_date": None,
        "outcome_close": None,
        "elapsed_sessions": prediction.get("elapsed_sessions"),
        "remaining_sessions": prediction.get("remaining_sessions"),
        "progress": prediction.get("progress"),
        "interim_direction": prediction.get("interim_direction"),
        "interim_status": prediction.get("interim_status"),
        "actual_direction": None,
        "is_correct": None,
        "current_trade_return": prediction.get("current_trade_return"),
        "current_trade_return_atr": prediction.get("current_trade_return_atr"),
        "trade_return": None,
        "trade_return_atr": None,
        "continuation_probability": prediction.get("continuation_probability"),
        "reversal_probability": prediction.get("reversal_probability"),
        "confidence_score": prediction.get("confidence_score"),
        "signal_model": prediction.get("signal_model"),
        "signal_model_id": prediction.get("signal_model_id"),
        "signal_precision": prediction.get("signal_precision"),
        "signal_tier": prediction.get("signal_tier"),
        "key_reasons": current_horizon.get("key_reasons") if current_horizon else None,
        "playbook": playbook,
    }


def _row_from_closed_prediction(symbol: str, payload: dict, horizon_key: str, prediction: dict, source: str) -> dict | None:
    horizon_days = _horizon_days_from_key(horizon_key, prediction.get("horizon_days"))
    signal_date = _date_text(prediction.get("signal_date"))
    predicted_direction = prediction.get("predicted_direction")
    if not horizon_days or not signal_date or predicted_direction not in ("continuation", "reversal"):
        return None

    return {
        **_signal_base(symbol, payload, source),
        "signal_date": signal_date,
        "horizon_days": horizon_days,
        "status": "closed",
        "touched_side": prediction.get("touched_side"),
        "predicted_direction": predicted_direction,
        "trade_direction": prediction.get("trade_direction"),
        "signal_close": prediction.get("signal_close"),
        "current_date": _date_text(prediction.get("outcome_date")),
        "current_close": prediction.get("outcome_close"),
        "outcome_date": _date_text(prediction.get("outcome_date")),
        "outcome_close": prediction.get("outcome_close"),
        "elapsed_sessions": horizon_days,
        "remaining_sessions": 0,
        "progress": 1.0,
        "interim_direction": prediction.get("actual_direction"),
        "interim_status": "closed",
        "actual_direction": prediction.get("actual_direction"),
        "is_correct": prediction.get("is_correct"),
        "current_trade_return": prediction.get("trade_return"),
        "current_trade_return_atr": prediction.get("trade_return_atr"),
        "trade_return": prediction.get("trade_return"),
        "trade_return_atr": prediction.get("trade_return_atr"),
        "continuation_probability": prediction.get("continuation_probability"),
        "reversal_probability": prediction.get("reversal_probability"),
        "confidence_score": prediction.get("confidence_score"),
        "signal_model": prediction.get("signal_model"),
        "signal_model_id": prediction.get("signal_model_id"),
        "signal_precision": prediction.get("signal_precision"),
        "signal_tier": prediction.get("signal_tier"),
        "key_reasons": None,
        "playbook": None,
    }


def open_entry_signal_rows_from_payload(symbol: str, payload: dict, *, source: str = "entry_decision") -> list[dict]:
    if not isinstance(payload, dict):
        return []
    rows: list[dict] = []
    for horizon_key, backtest in (payload.get("backtest_1y") or {}).items():
        if not isinstance(backtest, dict):
            continue
        for prediction in backtest.get("open_predictions") or []:
            if not isinstance(prediction, dict):
                continue
            row = _row_from_open_prediction(symbol, payload, horizon_key, prediction, source)
            if row is not None:
                rows.append(row)
    return rows


def closed_entry_signal_rows_from_payload(symbol: str, payload: dict, *, source: str = "entry_decision") -> dict[tuple[str, int], dict]:
    rows: dict[tuple[str, int], dict] = {}
    if not isinstance(payload, dict):
        return rows
    for horizon_key, backtest in (payload.get("backtest_1y") or {}).items():
        if not isinstance(backtest, dict):
            continue
        for prediction in backtest.get("predictions") or []:
            if not isinstance(prediction, dict):
                continue
            row = _row_from_closed_prediction(symbol, payload, horizon_key, prediction, source)
            if row is not None:
                rows[(row["signal_date"], int(row["horizon_days"]))] = row
    return rows


def sync_entry_signals_from_payload(symbol: str, payload: dict, *, source: str = "entry_decision") -> dict:
    normalized = normalize_symbol(symbol)
    if not normalized:
        return {"status": "skipped", "reason": "missing_symbol"}
    if not _payload_is_current_for_signal_sync(payload):
        return {"status": "skipped", "reason": "historical_payload"}

    open_rows = open_entry_signal_rows_from_payload(normalized, payload, source=source)
    closed_by_key = closed_entry_signal_rows_from_payload(normalized, payload, source=source)

    opened_or_refreshed = 0
    closed = 0
    for row in open_rows:
        upsert_entry_decision_signal(row)
        opened_or_refreshed += 1

    open_keys = {(row["signal_date"], int(row["horizon_days"])) for row in open_rows}
    for key in list_open_entry_signal_keys(normalized):
        if key in open_keys:
            continue
        closed_row = closed_by_key.get(key)
        if closed_row is None:
            continue
        upsert_entry_decision_signal(closed_row)
        closed += 1

    return {
        "status": "synced",
        "symbol": normalized,
        "open": opened_or_refreshed,
        "closed": closed,
    }


def safe_sync_entry_signals_from_payload(symbol: str, payload: dict, *, source: str = "entry_decision") -> dict:
    try:
        return sync_entry_signals_from_payload(symbol, payload, source=source)
    except Exception as exc:
        logger.exception("Entry signal sync failed for %s.", symbol)
        return {"status": "error", "symbol": normalize_symbol(symbol), "error": str(exc)}


def get_open_entry_signal_symbols(limit: int | None = None) -> list[str]:
    try:
        return _repo_open_symbols(limit=limit)
    except Exception:
        logger.exception("Open Entry Signal symbol lookup failed.")
        return []


def list_open_entry_signals(symbols: Iterable[str] | None = None, limit: int = 200) -> list[dict]:
    try:
        return _repo_list_open_signals(symbols=symbols, limit=limit)
    except Exception:
        logger.exception("Open Entry Signal lookup failed.")
        return []
