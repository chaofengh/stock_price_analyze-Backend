"""
Non-blocking Entry Decision cache warmer.

The request path reads only completed full-model payloads. Misses start or
prioritize a worker process and return a loading contract to the frontend.
Background alert preloads run only when the backend is idle.
"""

from __future__ import annotations

import copy
import contextlib
import logging
import multiprocessing
import os
import pickle
import tempfile
import threading
import time
from datetime import datetime, timedelta
from typing import Iterable

import pytz

from analysis.data_fetcher_utils import normalize_symbol, symbol_candidates
from analysis.data_preparation import prepare_stock_data
from analysis.trade_entry_evaluation import (
    build_entry_decision_context_from_frame,
    build_entry_decision_from_context,
    context_metadata_from_payload,
    entry_context_serving_allowed,
    entry_payload_serving_allowed,
    evaluate_entry_context_freshness,
    get_entry_decision_context,
    latest_required_price_date,
    refresh_payload_freshness,
)
from tasks.entry_signal_tasks import (
    get_open_entry_signal_symbols,
    safe_sync_entry_signals_from_payload,
)
from utils.serialization import convert_to_python_types

logger = logging.getLogger(__name__)

_CHICAGO_TZ = pytz.timezone("America/Chicago")

DEFAULT_BATCH_SIZE = max(1, int(os.getenv("ENTRY_DECISION_PRELOAD_BATCH_SIZE", "2")))
DEFAULT_MIN_IDLE_SECONDS = max(
    0.0,
    float(os.getenv("ENTRY_DECISION_PRELOAD_MIN_IDLE_SECONDS", "2.0")),
)
DEFAULT_FAILURE_BACKOFF_SECONDS = max(
    1,
    int(os.getenv("ENTRY_DECISION_PRELOAD_FAILURE_BACKOFF_SECONDS", "900")),
)
DEFAULT_INTERACTIVE_FAILURE_BACKOFF_SECONDS = max(
    1,
    int(os.getenv("ENTRY_DECISION_INTERACTIVE_PRELOAD_FAILURE_BACKOFF_SECONDS", "15")),
)
DEFAULT_WORKER_TIMEOUT_SECONDS = max(
    1.0,
    float(os.getenv("ENTRY_DECISION_PRELOAD_TIMEOUT_SECONDS", "300")),
)
DEFAULT_CLOSE_CUTOFF_MINUTES = max(
    0.0,
    float(os.getenv("ENTRY_DECISION_PRELOAD_CLOSE_CUTOFF_MINUTES", "5")),
)
_MAX_CACHE_ENTRIES = max(1, int(os.getenv("ENTRY_DECISION_PRELOAD_CACHE_SIZE", "128")))
_CONTEXT_SYMBOLS = ("QQQ", "XLK")
_TRAINING_HISTORY_PERIOD = "2y"
_RESOURCE_CAPPED_WORKER_SOURCES = {"startup", "after_close"}

_state_lock = threading.Lock()
_preload_lock = threading.Lock()

_active_requests = 0
_last_request_activity_at = 0.0
_global_retry_after = 0.0
_payload_cache: dict[tuple[str, str, str], dict] = {}
_context_cache: dict[tuple[str, str], dict] = {}
_failure_retry_after: dict[tuple[str, str, str], float] = {}
_interactive_failure_retry_after: dict[tuple[str, str, str], float] = {}
_alert_preload_queue: list[tuple[str, str, str]] = []
_alert_preload_queued: set[tuple[str, str, str]] = set()
_worker_process = None
_worker_result_path: str | None = None
_worker_started_at = 0.0
_worker_symbols: list[str] = []
_worker_as_of_date = ""
_worker_source = ""


def _full_preload_enabled() -> bool:
    raw = os.getenv("ENTRY_DECISION_PRELOAD_FULL_ENABLED")
    if raw is None:
        return True
    return raw.strip().lower() not in ("0", "false", "no", "off")


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, str(default))).strip())
    except (TypeError, ValueError):
        return default


def _startup_alert_preload_max_symbols() -> int:
    return _env_int("ENTRY_DECISION_STARTUP_ALERT_PRELOAD_MAX_SYMBOLS", -1)


def _after_close_alert_preload_max_symbols() -> int:
    return _env_int("ENTRY_DECISION_AFTER_CLOSE_ALERT_PRELOAD_MAX_SYMBOLS", -1)


def _after_close_open_signal_discovery_enabled() -> bool:
    raw = os.getenv("ENTRY_DECISION_AFTER_CLOSE_OPEN_SIGNAL_DISCOVERY_ENABLED")
    if raw is None:
        return True
    return raw.strip().lower() not in ("0", "false", "no", "off")


def _after_close_open_signal_discovery_symbols(source: str) -> list[str]:
    if source != "after_close" or not _after_close_open_signal_discovery_enabled():
        return []
    try:
        from database.ticker_repository import get_all_tickers

        return _normalize_symbols(get_all_tickers())
    except Exception:
        logger.exception("Open Entry Signal discovery ticker lookup failed.")
        return []


def _batch_size_for_pending(pending: list[str], max_symbols: int | None) -> int:
    if max_symbols is None:
        return DEFAULT_BATCH_SIZE
    if max_symbols < 0:
        return len(pending)
    return max_symbols


def _resource_capped_worker_batch_size() -> int:
    return max(
        1,
        _env_int("ENTRY_DECISION_RESOURCE_CAPPED_PRELOAD_BATCH_SIZE", DEFAULT_BATCH_SIZE),
    )


def _resource_capped_worker_pause_seconds() -> float:
    raw = os.getenv("ENTRY_DECISION_RESOURCE_CAPPED_PRELOAD_PAUSE_SECONDS", "0.5")
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return 0.5


def _preload_worker_nice_increment() -> int:
    return max(0, _env_int("ENTRY_DECISION_BACKGROUND_PRELOAD_NICE", 10))


def _auto_preload_market_hours_only() -> bool:
    raw = os.getenv("ENTRY_DECISION_PRELOAD_MARKET_HOURS_ONLY")
    if raw is None:
        return True
    return raw.strip().lower() not in ("0", "false", "no", "off")


def _auto_preload_close_cutoff() -> timedelta:
    raw = os.getenv("ENTRY_DECISION_PRELOAD_CLOSE_CUTOFF_MINUTES")
    if raw is None:
        minutes = DEFAULT_CLOSE_CUTOFF_MINUTES
    else:
        try:
            minutes = float(raw)
        except ValueError:
            minutes = DEFAULT_CLOSE_CUTOFF_MINUTES
    return timedelta(minutes=max(0.0, minutes))


def _as_chicago_time(value: datetime | None = None) -> datetime:
    if value is None:
        return datetime.now(_CHICAGO_TZ)
    if value.tzinfo is None:
        return _CHICAGO_TZ.localize(value)
    return value.astimezone(_CHICAGO_TZ)


def _format_chicago_time(value: datetime) -> str:
    return value.astimezone(_CHICAGO_TZ).strftime("%Y-%m-%d %H:%M:%S")


def _auto_preload_market_gate(now: datetime | None = None) -> dict:
    """
    Gate speculative Entry Decision work to regular market hours.

    Interactive user predictions intentionally bypass this gate.
    """
    now_chi = _as_chicago_time(now)
    if not _auto_preload_market_hours_only():
        return {"allowed": True, "reason": "market_hours_guard_disabled"}

    from tasks.daily_scan_tasks import (
        next_regular_market_run_time_chi,
        regular_market_session_bounds_chi,
    )

    next_run_at = next_regular_market_run_time_chi(now_chi)
    closed_result = {
        "allowed": False,
        "reason": "market_closed",
        "checked_at": _format_chicago_time(now_chi),
        "next_run_at": _format_chicago_time(next_run_at),
    }
    session_bounds = regular_market_session_bounds_chi(now_chi.date())
    if session_bounds is None:
        return closed_result

    open_dt, close_dt = session_bounds
    cutoff_dt = close_dt - _auto_preload_close_cutoff()
    market_metadata = {
        "market_open_at": _format_chicago_time(open_dt),
        "market_close_at": _format_chicago_time(close_dt),
        "preload_cutoff_at": _format_chicago_time(cutoff_dt),
        "next_run_at": _format_chicago_time(next_run_at),
    }
    if now_chi < open_dt:
        return {**closed_result, **market_metadata}
    if now_chi >= close_dt:
        return {**closed_result, **market_metadata}
    if now_chi >= cutoff_dt:
        return {
            "allowed": False,
            "reason": "market_close_cutoff",
            "checked_at": _format_chicago_time(now_chi),
            **market_metadata,
        }
    return {
        "allowed": True,
        "reason": "market_open",
        "checked_at": _format_chicago_time(now_chi),
        **market_metadata,
    }


def _auto_preload_skipped_result(
    market_gate: dict,
    *,
    worker_result: dict | None = None,
    terminated_worker: dict | None = None,
) -> dict:
    result = {
        "status": "skipped",
        "reason": market_gate.get("reason", "market_closed"),
    }
    for key in (
        "checked_at",
        "market_open_at",
        "market_close_at",
        "preload_cutoff_at",
        "next_run_at",
    ):
        if market_gate.get(key):
            result[key] = market_gate[key]
    if worker_result is not None:
        result["worker"] = worker_result
    if terminated_worker is not None:
        result["terminated_worker"] = terminated_worker
    return result


def _payload_is_full_model(payload: dict | None) -> bool:
    if not isinstance(payload, dict):
        return False
    meta = payload.get("meta")
    if isinstance(meta, dict) and meta.get("full_decision_preloaded") is False:
        return False
    return True


def _payload_can_be_served(payload: dict | None) -> bool:
    if not isinstance(payload, dict):
        return False
    if not _payload_is_full_model(payload):
        return False
    return entry_payload_serving_allowed(payload)


def _context_can_be_served(context: dict | None) -> bool:
    return entry_context_serving_allowed(context)


def _cache_day() -> str:
    return latest_required_price_date() or datetime.now(_CHICAGO_TZ).strftime("%Y-%m-%d")


def _metadata_price_data_end_date(metadata: dict | None) -> str | None:
    if not isinstance(metadata, dict):
        return None
    value = metadata.get("price_data_end_date")
    if not value:
        return None
    text = str(value)[:10]
    try:
        datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        return None
    return text


def _payload_has_completed_price_date_for_cache_day(
    payload: dict | None,
    cache_day: str,
) -> bool:
    price_end = _metadata_price_data_end_date(context_metadata_from_payload(payload))
    return price_end is not None and price_end >= cache_day


def _cached_payload_can_satisfy_cache_day(cached: dict | None, cache_day: str) -> bool:
    if not isinstance(cached, dict):
        return False
    payload = cached.get("payload")
    return _payload_can_be_served(payload) and _payload_has_completed_price_date_for_cache_day(payload, cache_day)


def _context_has_completed_price_date_for_cache_day(
    context: dict | None,
    cache_day: str,
) -> bool:
    metadata = context.get("meta") if isinstance(context, dict) else None
    price_end = _metadata_price_data_end_date(metadata)
    return price_end is not None and price_end >= cache_day


def _normalize_as_of_date(as_of_date: str | None = None) -> str:
    text = str(as_of_date or "").strip()
    return text or _cache_day()


def _alert_payload_as_of_date(alert_payload: dict | None) -> str:
    if not isinstance(alert_payload, dict):
        return _cache_day()
    timestamp = str(alert_payload.get("timestamp") or "").strip()
    if len(timestamp) >= 10:
        candidate = timestamp[:10]
        try:
            datetime.strptime(candidate, "%Y-%m-%d")
            return candidate
        except ValueError:
            pass
    return _cache_day()


def _cache_key(symbol: str, as_of_date: str | None = None) -> tuple[str, str, str]:
    return _cache_day(), normalize_symbol(symbol), _normalize_as_of_date(as_of_date)


def _context_key(symbol: str) -> tuple[str, str]:
    return _cache_day(), normalize_symbol(symbol)


def _prune_cache_locked() -> None:
    if len(_payload_cache) <= _MAX_CACHE_ENTRIES:
        payload_keys_to_prune = []
    else:
        ordered = sorted(
            _payload_cache.items(),
            key=lambda item: item[1].get("loaded_at", 0.0),
        )
        payload_keys_to_prune = [key for key, _ in ordered[: len(_payload_cache) - _MAX_CACHE_ENTRIES]]

    for key in payload_keys_to_prune:
        _payload_cache.pop(key, None)
        _failure_retry_after.pop(key, None)
        _interactive_failure_retry_after.pop(key, None)
        _alert_preload_queued.discard(key)
    _alert_preload_queue[:] = [key for key in _alert_preload_queue if key in _alert_preload_queued]

    if len(_context_cache) <= _MAX_CACHE_ENTRIES:
        return
    ordered_contexts = sorted(
        _context_cache.items(),
        key=lambda item: item[1].get("loaded_at", 0.0),
    )
    for key, _ in ordered_contexts[: len(_context_cache) - _MAX_CACHE_ENTRIES]:
        _context_cache.pop(key, None)


def _normalize_symbols(symbols: Iterable[str] | None) -> list[str]:
    normalized: list[str] = []
    seen = set()
    for symbol in symbols or []:
        value = normalize_symbol(str(symbol))
        if not value or value in seen:
            continue
        normalized.append(value)
        seen.add(value)
    return normalized


def alert_symbols_from_payload(payload: dict | None) -> list[str]:
    if not isinstance(payload, dict):
        return []
    symbols = []
    for collection_key in ("alerts", "open_entry_signals"):
        for item in payload.get(collection_key) or []:
            if isinstance(item, dict):
                symbol = item.get("symbol") or item.get("ticker")
            else:
                symbol = item
            if symbol:
                symbols.append(symbol)
    return _normalize_symbols(symbols)

def mark_backend_request_started() -> None:
    global _active_requests, _last_request_activity_at
    with _state_lock:
        _active_requests += 1
        _last_request_activity_at = time.monotonic()


def mark_backend_request_finished() -> None:
    global _active_requests, _last_request_activity_at
    with _state_lock:
        _active_requests = max(0, _active_requests - 1)
        _last_request_activity_at = time.monotonic()


def is_backend_idle(min_idle_seconds: float | None = None) -> bool:
    min_idle_seconds = DEFAULT_MIN_IDLE_SECONDS if min_idle_seconds is None else min_idle_seconds
    now = time.monotonic()
    with _state_lock:
        if _active_requests > 0:
            return False
        if _last_request_activity_at <= 0:
            return True
        return now - _last_request_activity_at >= min_idle_seconds


def _copy_cached_payload_locked(key: tuple[str, str, str], *, full_only: bool = False) -> dict | None:
    cached = _payload_cache.get(key)
    if cached is None:
        return None
    payload = cached.get("payload")
    if full_only and not _payload_is_full_model(payload):
        return None
    if not entry_payload_serving_allowed(payload) or not _payload_has_completed_price_date_for_cache_day(payload, key[0]):
        _payload_cache.pop(key, None)
        _failure_retry_after.pop(key, None)
        _interactive_failure_retry_after.pop(key, None)
        _alert_preload_queued.discard(key)
        _alert_preload_queue[:] = [queued_key for queued_key in _alert_preload_queue if queued_key != key]
        return None
    payload_copy = copy.deepcopy(cached["payload"])
    return refresh_payload_freshness(payload_copy)


def get_preloaded_entry_decision(
    symbol: str,
    as_of_date: str | None = None,
    *,
    full_only: bool = False,
) -> dict | None:
    normalized = normalize_symbol(symbol)
    if not normalized:
        return None
    with _state_lock:
        exact_payload = _copy_cached_payload_locked(_cache_key(normalized, as_of_date), full_only=full_only)
        if exact_payload is not None:
            return exact_payload
        context_cache_key = _context_key(normalized)
        cached_context = _context_cache.get(context_cache_key)
        context = cached_context.get("context") if isinstance(cached_context, dict) else None
        if context is not None and (
            not _context_can_be_served(context)
            or not _context_has_completed_price_date_for_cache_day(context, context_cache_key[0])
        ):
            _context_cache.pop(context_cache_key, None)
            context = None
        if context is None and not as_of_date:
            latest_payload = _copy_cached_payload_locked(_cache_key(normalized, _cache_day()), full_only=full_only)
            if latest_payload is not None:
                return latest_payload

    if context is None:
        return None

    payload = convert_to_python_types(
        build_entry_decision_from_context(
            context,
            as_of_date=as_of_date,
        )
    )
    store_preloaded_entry_decision(normalized, payload, as_of_date=as_of_date)
    return payload


def store_entry_decision_context(symbol: str, context: dict) -> bool:
    normalized = normalize_symbol(symbol)
    if not normalized or not isinstance(context, dict):
        return False
    with _state_lock:
        freshness = evaluate_entry_context_freshness(context.get("meta"))
        _context_cache[_context_key(normalized)] = {
            "context": context,
            "loaded_at": time.time(),
            "context_key": context.get("meta", {}).get("context_key"),
            "freshness": freshness,
        }
        _prune_cache_locked()
    return True


def store_preloaded_entry_decision(
    symbol: str,
    payload: dict,
    *,
    as_of_date: str | None = None,
) -> bool:
    if not isinstance(payload, dict):
        return False
    normalized = normalize_symbol(symbol)
    if not normalized:
        return False
    key = _cache_key(normalized, as_of_date)
    with _state_lock:
        payload_to_store = copy.deepcopy(payload)
        refresh_payload_freshness(payload_to_store)
        _payload_cache[key] = {
            "payload": payload_to_store,
            "loaded_at": time.time(),
        }
        _failure_retry_after.pop(key, None)
        _interactive_failure_retry_after.pop(key, None)
        _alert_preload_queued.discard(key)
        _alert_preload_queue[:] = [queued_key for queued_key in _alert_preload_queue if queued_key != key]
        _prune_cache_locked()
    return True


def _symbols_needing_preload(
    symbols: list[str],
    *,
    as_of_date: str | None = None,
    full_only: bool = False,
) -> list[str]:
    now = time.time()
    preload_date = _normalize_as_of_date(as_of_date)
    pending = []
    with _state_lock:
        for symbol in symbols:
            key = _cache_key(symbol, preload_date)
            if _failure_retry_after.get(key, 0.0) > now:
                continue
            context_key = _context_key(symbol)
            cached_context = _context_cache.get(context_key)
            context = cached_context.get("context") if isinstance(cached_context, dict) else None
            if context is not None:
                if _context_can_be_served(context) and _context_has_completed_price_date_for_cache_day(context, context_key[0]):
                    continue
                _context_cache.pop(context_key, None)
            cached = _payload_cache.get(key)
            if not _cached_payload_can_satisfy_cache_day(cached, key[0]):
                pending.append(symbol)
                continue
            if full_only and not _payload_is_full_model(cached.get("payload")):
                pending.append(symbol)
    return pending


def _key_needs_full_preload_locked(key: tuple[str, str, str], now: float | None = None) -> bool:
    cache_day, _symbol, _as_of_date = key
    if cache_day != _cache_day():
        return False
    now = time.time() if now is None else now
    if _failure_retry_after.get(key, 0.0) > now:
        return False
    cached_context = _context_cache.get((cache_day, _symbol))
    context = cached_context.get("context") if isinstance(cached_context, dict) else None
    if context is not None:
        if _context_can_be_served(context) and _context_has_completed_price_date_for_cache_day(context, cache_day):
            return False
        _context_cache.pop((cache_day, _symbol), None)
    cached = _payload_cache.get(key)
    if not _cached_payload_can_satisfy_cache_day(cached, cache_day):
        return True
    return not _payload_is_full_model(cached.get("payload"))


def _running_worker_keys_locked() -> set[tuple[str, str, str]]:
    if _worker_process is None:
        return set()
    return {_cache_key(symbol, _worker_as_of_date) for symbol in _worker_symbols}


def _running_worker_can_satisfy_interactive_request(
    normalized: str,
    preload_date: str,
    worker_symbols: list[str],
    worker_as_of_date: str,
    worker_source: str,
) -> bool:
    if worker_as_of_date != preload_date or worker_symbols != [normalized]:
        return False
    return worker_source in ("interactive", "alert")


def _queue_alert_preload_symbols(
    symbols: list[str],
    as_of_date: str,
    *,
    front: bool = False,
) -> list[str]:
    """
    Remember user-visible alert symbols that still need a full model.

    The queue lets /api/alerts/latest return immediately even when another
    worker is already running. The scheduler drains this queue before generic
    speculative work so user-visible alert tickers get priority.
    """
    queued: list[str] = []
    queued_keys: list[tuple[str, str, str]] = []
    now = time.time()
    with _state_lock:
        running_keys = _running_worker_keys_locked()
        for symbol in _normalize_symbols(symbols):
            key = _cache_key(symbol, as_of_date)
            if key in running_keys:
                continue
            if not _key_needs_full_preload_locked(key, now):
                continue
            if key in _alert_preload_queued and not front:
                continue
            _alert_preload_queued.add(key)
            queued_keys.append(key)
            queued.append(symbol)
        if front and queued_keys:
            prioritized = set(queued_keys)
            _alert_preload_queue[:] = queued_keys + [
                key for key in _alert_preload_queue if key not in prioritized
            ]
        else:
            _alert_preload_queue.extend(queued_keys)
    return queued


def _next_queued_alert_batch(max_symbols: int | None = None) -> tuple[list[str], str] | None:
    batch_limit: int | None
    if max_symbols is None:
        batch_limit = DEFAULT_BATCH_SIZE
    elif max_symbols < 0:
        batch_limit = None
    else:
        batch_limit = max_symbols
    if batch_limit == 0:
        return None

    now = time.time()
    with _state_lock:
        if not _alert_preload_queue:
            return None

        selected_symbols: list[str] = []
        selected_cache_day = ""
        selected_as_of_date = ""
        retained: list[tuple[str, str, str]] = []

        for key in _alert_preload_queue:
            _alert_preload_queued.discard(key)
            if not _key_needs_full_preload_locked(key, now):
                continue

            cache_day, symbol, as_of_date = key
            if not selected_symbols:
                selected_cache_day = cache_day
                selected_as_of_date = as_of_date

            same_batch = cache_day == selected_cache_day and as_of_date == selected_as_of_date
            under_limit = batch_limit is None or len(selected_symbols) < batch_limit
            if same_batch and under_limit:
                selected_symbols.append(symbol)
                continue

            retained.append(key)
            _alert_preload_queued.add(key)

        _alert_preload_queue[:] = retained

    if not selected_symbols:
        return None
    return selected_symbols, selected_as_of_date


def _start_next_queued_alert_preload(max_symbols: int | None = None) -> dict | None:
    batch = _next_queued_alert_batch(max_symbols=max_symbols)
    if batch is None:
        return None
    batch_symbols, as_of_date = batch

    if os.getenv("ENTRY_DECISION_PRELOAD_INLINE", "").strip() == "1":
        loaded_payloads, contexts, failed = _compute_interactive_artifacts(batch_symbols, as_of_date)
        return _store_worker_results(loaded_payloads, failed, as_of_date, source="alert", contexts=contexts)

    try:
        _start_preload_worker(batch_symbols, as_of_date, source="alert")
    except Exception as exc:
        for symbol in batch_symbols:
            _mark_preload_failed(symbol, as_of_date=as_of_date)
        logger.exception("Entry decision alert preload worker failed to start for %s", batch_symbols)
        return {
            "status": "error",
            "reason": "preload_start_failed",
            "symbols": batch_symbols,
            "error": str(exc),
            "retry_after_seconds": DEFAULT_FAILURE_BACKOFF_SECONDS,
        }
    return {"status": "started", "symbols": batch_symbols, "source": "alert"}


def _mark_preload_failed(symbol: str, as_of_date: str | None = None) -> None:
    key = _cache_key(symbol, as_of_date)
    with _state_lock:
        _failure_retry_after[key] = time.time() + DEFAULT_FAILURE_BACKOFF_SECONDS


def _mark_interactive_preload_failed(symbol: str, as_of_date: str | None = None) -> None:
    key = _cache_key(symbol, as_of_date)
    with _state_lock:
        _interactive_failure_retry_after[key] = time.time() + DEFAULT_INTERACTIVE_FAILURE_BACKOFF_SECONDS


def _mark_global_preload_failed() -> None:
    global _global_retry_after
    with _state_lock:
        _global_retry_after = time.time() + DEFAULT_FAILURE_BACKOFF_SECONDS


def _global_preload_backoff_remaining() -> float:
    with _state_lock:
        return max(0.0, _global_retry_after - time.time())


def _symbol_preload_backoff_remaining(symbol: str, as_of_date: str) -> float:
    with _state_lock:
        return _symbol_preload_backoff_remaining_locked(symbol, as_of_date)


def _symbol_preload_backoff_remaining_locked(symbol: str, as_of_date: str) -> float:
    key = _cache_key(symbol, as_of_date)
    return max(0.0, _failure_retry_after.get(key, 0.0) - time.time())


def _interactive_symbol_backoff_remaining(symbol: str, as_of_date: str) -> float:
    key = _cache_key(symbol, as_of_date)
    with _state_lock:
        return max(0.0, _interactive_failure_retry_after.get(key, 0.0) - time.time())


def _is_valid_price_frame(frame) -> bool:
    return frame is not None and not getattr(frame, "empty", True) and "close" in frame.columns


def _download_symbol_list(symbols: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen = set()
    for symbol in symbols:
        for candidate in symbol_candidates(symbol):
            if candidate and candidate not in seen:
                out.append(candidate)
                seen.add(candidate)
    for context_symbol in _CONTEXT_SYMBOLS:
        if context_symbol not in seen:
            out.append(context_symbol)
            seen.add(context_symbol)
    return out


def _compute_preload_artifacts(
    symbols: list[str],
    as_of_date: str,
) -> tuple[dict[str, dict], dict[str, dict], dict[str, str]]:
    """
    Build preload payloads and reusable model contexts from one batched OHLC download.

    This deliberately avoids the request path's single-ticker history fallback,
    but it still resolves earnings dates. Alert preloads are user-visible model
    output, so they must honor the same event-risk blackout as interactive calls.
    """
    loaded: dict[str, dict] = {}
    contexts: dict[str, dict] = {}
    failed: dict[str, str] = {}
    download_symbols = _download_symbol_list(symbols)
    if not download_symbols:
        return loaded, contexts, failed

    try:
        data_dict = prepare_stock_data(
            download_symbols,
            include_rsi=False,
            period=_TRAINING_HISTORY_PERIOD,
            interval="1d",
            threads=False,
        )
    except Exception as exc:
        failed = {symbol: str(exc) for symbol in symbols}
        failed["__global__"] = str(exc)
        return loaded, contexts, failed

    if not any(_is_valid_price_frame(data_dict.get(symbol)) for symbol in download_symbols):
        failed = {symbol: "Preload price download returned no usable frames." for symbol in symbols}
        failed["__global__"] = "Preload price download returned no usable frames."
        return loaded, contexts, failed

    context_frames = {
        context_symbol: data_dict.get(context_symbol)
        for context_symbol in _CONTEXT_SYMBOLS
        if _is_valid_price_frame(data_dict.get(context_symbol))
    }

    for symbol in symbols:
        resolved_symbol = ""
        frame = None
        for candidate in symbol_candidates(symbol):
            candidate_frame = data_dict.get(candidate)
            if _is_valid_price_frame(candidate_frame):
                resolved_symbol = candidate
                frame = candidate_frame
                break
        if frame is None:
            failed[symbol] = "No preload price data returned."
            continue

        try:
            context = build_entry_decision_context_from_frame(
                symbol,
                frame,
                earnings_dates=None,
                earnings_symbol=resolved_symbol or symbol,
                context_frames=context_frames,
                price_data_cutoff_date=_cache_day(),
            )
            payload = build_entry_decision_from_context(context, as_of_date=as_of_date)
            loaded[symbol] = convert_to_python_types(payload)
            contexts[symbol] = context
        except Exception as exc:
            failed[symbol] = str(exc)
    return loaded, contexts, failed


def _compute_preload_payloads(symbols: list[str], as_of_date: str) -> tuple[dict[str, dict], dict[str, str]]:
    loaded, _contexts, failed = _compute_preload_artifacts(symbols, as_of_date)
    return loaded, failed


def _merge_preload_results(
    target_loaded: dict[str, dict],
    target_contexts: dict[str, dict],
    target_failed: dict[str, str],
    loaded: dict[str, dict],
    contexts: dict[str, dict],
    failed: dict[str, str],
) -> None:
    target_loaded.update(loaded)
    target_contexts.update(contexts)
    for symbol, error in failed.items():
        if symbol == "__global__" and symbol in target_failed:
            target_failed[symbol] = f"{target_failed[symbol]}; {error}"
        else:
            target_failed[symbol] = error


def _compute_resource_capped_preload_artifacts(
    symbols: list[str],
    as_of_date: str,
) -> tuple[dict[str, dict], dict[str, dict], dict[str, str]]:
    loaded: dict[str, dict] = {}
    contexts: dict[str, dict] = {}
    failed: dict[str, str] = {}
    batch_size = _resource_capped_worker_batch_size()
    pause_seconds = _resource_capped_worker_pause_seconds()

    for start in range(0, len(symbols), batch_size):
        batch_symbols = symbols[start : start + batch_size]
        batch_loaded, batch_contexts, batch_failed = _compute_preload_artifacts(
            batch_symbols,
            as_of_date,
        )
        _merge_preload_results(loaded, contexts, failed, batch_loaded, batch_contexts, batch_failed)
        if pause_seconds > 0 and start + batch_size < len(symbols):
            time.sleep(pause_seconds)

    return loaded, contexts, failed


def _compute_interactive_artifacts(
    symbols: list[str],
    as_of_date: str,
) -> tuple[dict[str, dict], dict[str, dict], dict[str, str]]:
    """
    Build the expensive reusable model context once, then render the selected
    date from that context.
    """
    loaded: dict[str, dict] = {}
    contexts: dict[str, dict] = {}
    failed: dict[str, str] = {}
    for symbol in symbols:
        try:
            context = get_entry_decision_context(symbol)
            payload = build_entry_decision_from_context(context, as_of_date=as_of_date)
            loaded[symbol] = convert_to_python_types(payload)
            contexts[symbol] = context
        except Exception as exc:
            failed[symbol] = str(exc)
    return loaded, contexts, failed


def _compute_interactive_payloads(symbols: list[str], as_of_date: str) -> tuple[dict[str, dict], dict[str, str]]:
    loaded, _contexts, failed = _compute_interactive_artifacts(symbols, as_of_date)
    return loaded, failed


def _apply_preload_worker_resource_limits(source: str) -> None:
    if source not in _RESOURCE_CAPPED_WORKER_SOURCES:
        return
    nice_increment = _preload_worker_nice_increment()
    if nice_increment <= 0:
        return
    try:
        os.nice(nice_increment)
    except OSError:
        logger.debug("Entry Decision preload worker priority could not be lowered.", exc_info=True)


def _child_compute_preload_payloads(result_path: str, symbols: list[str], as_of_date: str, source: str) -> None:
    with (
        open(os.devnull, "w") as devnull,
        contextlib.redirect_stdout(devnull),
        contextlib.redirect_stderr(devnull),
    ):
        try:
            _apply_preload_worker_resource_limits(source)
            if source in ("interactive", "alert"):
                loaded, contexts, failed = _compute_interactive_artifacts(symbols, as_of_date)
            elif source in _RESOURCE_CAPPED_WORKER_SOURCES:
                loaded, contexts, failed = _compute_resource_capped_preload_artifacts(symbols, as_of_date)
            else:
                loaded, contexts, failed = _compute_preload_artifacts(symbols, as_of_date)
        except Exception as exc:  # pragma: no cover - defensive child-process guard
            loaded = {}
            contexts = {}
            failed = {symbol: str(exc) for symbol in symbols}
            failed["__global__"] = str(exc)
    with open(result_path, "wb") as handle:
        pickle.dump((loaded, contexts, failed), handle, protocol=pickle.HIGHEST_PROTOCOL)


def _cleanup_worker_file(path: str | None) -> None:
    if not path:
        return
    try:
        os.unlink(path)
    except OSError:
        pass


def _clear_worker_state() -> None:
    global _worker_process, _worker_result_path, _worker_started_at, _worker_symbols, _worker_as_of_date, _worker_source
    _cleanup_worker_file(_worker_result_path)
    _worker_process = None
    _worker_result_path = None
    _worker_started_at = 0.0
    _worker_symbols = []
    _worker_as_of_date = ""
    _worker_source = ""


def _terminate_worker() -> None:
    process = _worker_process
    if process is not None and process.is_alive():
        process.terminate()
        process.join(timeout=1)
        if process.is_alive():  # pragma: no cover - platform defensive guard
            process.kill()
            process.join(timeout=1)
    elif process is not None:
        process.join(timeout=1)
    _clear_worker_state()


def _terminate_noninteractive_worker_for_market_gate(reason: str) -> dict | None:
    process = _worker_process
    if process is None or _worker_source == "interactive" or not process.is_alive():
        return None

    terminated = {
        "status": "terminated",
        "reason": reason,
        "source": _worker_source or "background",
        "symbols": list(_worker_symbols),
        "as_of_date": _worker_as_of_date,
    }
    _terminate_worker()
    return terminated


def _start_preload_worker(symbols: list[str], as_of_date: str, *, source: str = "background") -> None:
    global _worker_process, _worker_result_path, _worker_started_at, _worker_symbols, _worker_as_of_date, _worker_source
    result_file = tempfile.NamedTemporaryFile(
        prefix="entry_decision_preload_",
        suffix=".pickle",
        delete=False,
    )
    result_path = result_file.name
    result_file.close()

    ctx = multiprocessing.get_context(os.getenv("ENTRY_DECISION_PRELOAD_MP_CONTEXT", "spawn"))
    process = ctx.Process(
        target=_child_compute_preload_payloads,
        args=(result_path, symbols, as_of_date, source),
    )
    process.daemon = True
    process.start()

    _worker_process = process
    _worker_result_path = result_path
    _worker_started_at = time.monotonic()
    _worker_symbols = list(symbols)
    _worker_as_of_date = as_of_date
    _worker_source = source


def _store_worker_results(
    loaded_payloads: dict[str, dict],
    failed: dict[str, str],
    as_of_date: str,
    *,
    source: str,
    contexts: dict[str, dict] | None = None,
) -> dict:
    loaded: list[str] = []
    signal_sync: dict[str, dict] = {}
    for symbol, context in (contexts or {}).items():
        store_entry_decision_context(symbol, context)

    for symbol, payload in loaded_payloads.items():
        store_preloaded_entry_decision(symbol, payload, as_of_date=as_of_date)
        sync_result = safe_sync_entry_signals_from_payload(symbol, payload, source=source)
        if sync_result.get("status") != "skipped":
            signal_sync[symbol] = sync_result
        loaded.append(symbol)

    if source in ("background", "startup", "after_close") and "__global__" in failed:
        _mark_global_preload_failed()
    failed.pop("__global__", None)
    for symbol in failed:
        if source == "interactive":
            _mark_interactive_preload_failed(symbol, as_of_date=as_of_date)
        else:
            _mark_preload_failed(symbol, as_of_date=as_of_date)
        logger.debug("Entry decision preload skipped for %s: %s", symbol, failed[symbol])

    if loaded:
        result = {
            "status": "loaded",
            "loaded": len(loaded),
            "symbols": loaded,
            "failed": failed,
        }
        if signal_sync:
            result["entry_signal_sync"] = signal_sync
        return result
    if failed:
        return {"status": "error", "loaded": 0, "failed": failed}
    return {"status": "ready", "loaded": 0, "symbols": []}


def _reap_preload_worker(timeout_seconds: float) -> dict | None:
    process = _worker_process
    if process is None:
        return None

    elapsed = time.monotonic() - _worker_started_at
    if process.is_alive():
        if elapsed < timeout_seconds:
            return {
                "status": "running",
                "symbols": list(_worker_symbols),
                "elapsed_seconds": int(elapsed),
                "source": _worker_source,
            }
        timed_out_symbols = list(_worker_symbols)
        timed_out_as_of_date = _worker_as_of_date
        timed_out_source = _worker_source
        _terminate_worker()
        if timed_out_source == "background":
            _mark_global_preload_failed()
        for symbol in timed_out_symbols:
            if timed_out_source == "interactive":
                _mark_interactive_preload_failed(symbol, as_of_date=timed_out_as_of_date)
            else:
                _mark_preload_failed(symbol, as_of_date=timed_out_as_of_date)
        return {
            "status": "skipped",
            "reason": "preload_worker_timeout",
            "symbols": timed_out_symbols,
            "source": timed_out_source,
        }

    process.join(timeout=1)
    result_path = _worker_result_path
    as_of_date = _worker_as_of_date
    source = _worker_source or "background"
    try:
        if not result_path or not os.path.exists(result_path) or os.path.getsize(result_path) <= 0:
            failed = {symbol: f"worker exited with code {process.exitcode}" for symbol in _worker_symbols}
            failed["__global__"] = f"worker exited with code {process.exitcode}"
            return _store_worker_results({}, failed, as_of_date, source=source)
        with open(result_path, "rb") as handle:
            result = pickle.load(handle)
        if isinstance(result, tuple) and len(result) == 3:
            loaded_payloads, contexts, failed = result
        else:
            loaded_payloads, failed = result
            contexts = {}
        return _store_worker_results(loaded_payloads, failed, as_of_date, source=source, contexts=contexts)
    finally:
        _clear_worker_state()


def _run_preload_inline(batch_symbols: list[str], as_of_date: str) -> dict:
    loaded_payloads, contexts, failed = _compute_preload_artifacts(batch_symbols, as_of_date)
    return _store_worker_results(loaded_payloads, failed, as_of_date, source="background", contexts=contexts)


def refresh_entry_decision_preload_state(timeout_seconds: float | None = None) -> dict | None:
    timeout_seconds = DEFAULT_WORKER_TIMEOUT_SECONDS if timeout_seconds is None else timeout_seconds
    if not _preload_lock.acquire(blocking=False):
        return {"status": "skipped", "reason": "preload_coordinator_busy"}
    try:
        return _reap_preload_worker(timeout_seconds)
    finally:
        _preload_lock.release()


def request_full_entry_decision_preload(
    symbol: str,
    as_of_date: str | None = None,
    *,
    force: bool = False,
    ignore_backoff: bool = False,
) -> dict:
    normalized = normalize_symbol(symbol)
    if not normalized:
        return {"status": "skipped", "reason": "missing_symbol"}
    if not force and not _full_preload_enabled():
        return {"status": "skipped", "reason": "full_preload_disabled"}

    preload_date = _normalize_as_of_date(as_of_date)
    if not _preload_lock.acquire(blocking=False):
        return {"status": "skipped", "reason": "preload_coordinator_busy"}
    try:
        worker_result = _reap_preload_worker(DEFAULT_WORKER_TIMEOUT_SECONDS)
        if get_preloaded_entry_decision(normalized, as_of_date=preload_date, full_only=True) is not None:
            return {"status": "ready", "symbol": normalized, "worker": worker_result}
        if _worker_process is not None:
            worker_symbols = list(_worker_symbols)
            worker_as_of_date = _worker_as_of_date
            worker_source = _worker_source
            if not force and normalized in worker_symbols and worker_as_of_date == preload_date:
                return {
                    "status": "running",
                    "symbol": normalized,
                    "worker": worker_result,
                    "source": worker_source,
                }
            if force and _running_worker_can_satisfy_interactive_request(
                normalized,
                preload_date,
                worker_symbols,
                worker_as_of_date,
                worker_source,
            ):
                return {
                    "status": "running",
                    "symbol": normalized,
                    "worker": worker_result,
                    "source": worker_source,
                }
            symbol_cooldown_remaining = (
                _interactive_symbol_backoff_remaining(normalized, preload_date)
                if force
                else _symbol_preload_backoff_remaining(normalized, preload_date)
            )
            if symbol_cooldown_remaining > 0:
                return {
                    "status": "skipped",
                    "reason": "interactive_symbol_preload_backoff" if force else "symbol_preload_backoff",
                    "retry_after_seconds": int(symbol_cooldown_remaining),
                }
            if force:
                # The latest user-opened ticker outranks older speculative work.
                preempted_symbols = worker_symbols
                preempted_as_of_date = worker_as_of_date
                preempted_source = worker_source
                _terminate_worker()
                if preempted_source == "alert":
                    _queue_alert_preload_symbols(preempted_symbols, preempted_as_of_date, front=True)
            else:
                return {
                    "status": "running",
                    "symbol": normalized,
                    "reason": "worker_running",
                    "active_symbols": worker_symbols,
                    "worker": worker_result,
                    "source": worker_source,
                }
        symbol_cooldown_remaining = (
            _interactive_symbol_backoff_remaining(normalized, preload_date)
            if force
            else _symbol_preload_backoff_remaining(normalized, preload_date)
        )
        if symbol_cooldown_remaining > 0:
            return {
                "status": "skipped",
                "reason": "interactive_symbol_preload_backoff" if force else "symbol_preload_backoff",
                "retry_after_seconds": int(symbol_cooldown_remaining),
            }
        cooldown_remaining = _global_preload_backoff_remaining()
        if cooldown_remaining > 0 and not ignore_backoff:
            return {
                "status": "skipped",
                "reason": "preload_source_backoff",
                "retry_after_seconds": int(cooldown_remaining),
            }
        try:
            _start_preload_worker([normalized], preload_date, source="interactive" if force else "background")
        except Exception as exc:
            if force:
                _mark_interactive_preload_failed(normalized, as_of_date=preload_date)
            else:
                _mark_preload_failed(normalized, as_of_date=preload_date)
            logger.exception("Entry decision preload worker failed to start for %s", normalized)
            return {
                "status": "error",
                "reason": "preload_start_failed",
                "symbol": normalized,
                "error": str(exc),
                "retry_after_seconds": (
                    DEFAULT_INTERACTIVE_FAILURE_BACKOFF_SECONDS if force else DEFAULT_FAILURE_BACKOFF_SECONDS
                ),
            }
        return {"status": "started", "symbol": normalized}
    finally:
        _preload_lock.release()


def preload_entry_decisions_from_latest_alerts(
    *,
    max_symbols: int | None = None,
    min_idle_seconds: float | None = None,
    timeout_seconds: float | None = None,
    respect_market_gate: bool = True,
    source: str = "background",
) -> dict:
    """
    Warm the latest Entry Decision payload cache for current Bollinger alert symbols.

    This is intended to run from APScheduler. The scheduler call is only a
    coordinator: it starts or reaps one worker process and returns immediately.
    """
    timeout_seconds = DEFAULT_WORKER_TIMEOUT_SECONDS if timeout_seconds is None else timeout_seconds
    if not _preload_lock.acquire(blocking=False):
        return {"status": "skipped", "reason": "preload_coordinator_busy"}

    try:
        market_gate = (
            _auto_preload_market_gate()
            if respect_market_gate
            else {"allowed": True, "reason": "market_gate_bypassed"}
        )
        terminated_worker = None
        if not market_gate.get("allowed", False):
            terminated_worker = _terminate_noninteractive_worker_for_market_gate(
                market_gate.get("reason", "market_closed")
            )

        worker_result = _reap_preload_worker(timeout_seconds)
        if not market_gate.get("allowed", False):
            return _auto_preload_skipped_result(
                market_gate,
                worker_result=worker_result,
                terminated_worker=terminated_worker,
            )

        if isinstance(worker_result, dict) and worker_result.get("status") == "running":
            return worker_result

        if not is_backend_idle(min_idle_seconds):
            return {"status": "skipped", "reason": "backend_busy"}

        queued_alert_result = _start_next_queued_alert_preload(max_symbols=max_symbols)
        if queued_alert_result is not None:
            if worker_result is not None:
                queued_alert_result["previous_worker"] = worker_result
            return queued_alert_result

        cooldown_remaining = _global_preload_backoff_remaining()
        if cooldown_remaining > 0:
            return {
                "status": "skipped",
                "reason": "preload_source_backoff",
                "retry_after_seconds": int(cooldown_remaining),
            }

        from tasks.daily_scan_tasks import get_cached_scan_result

        alert_payload = get_cached_scan_result()
        background_as_of_date = _alert_payload_as_of_date(alert_payload)
        result = _preload_entry_decisions_from_alert_payload_locked(
            alert_payload,
            max_symbols=max_symbols,
            source=source,
            as_of_date=background_as_of_date,
        )
        if worker_result is not None and result.get("status") in ("ready", "skipped"):
            return worker_result
        if worker_result is not None:
            result["previous_worker"] = worker_result
        return result
    finally:
        _preload_lock.release()


def preload_entry_decisions_for_startup_alerts() -> dict:
    return preload_entry_decisions_from_latest_alerts(
        max_symbols=_startup_alert_preload_max_symbols(),
        min_idle_seconds=0,
        respect_market_gate=False,
        source="startup",
    )


def refresh_entry_decisions_for_latest_alerts_after_close() -> dict:
    return preload_entry_decisions_from_latest_alerts(
        max_symbols=_after_close_alert_preload_max_symbols(),
        min_idle_seconds=0,
        respect_market_gate=False,
        source="after_close",
    )


def _preload_entry_decisions_from_alert_payload_locked(
    alert_payload: dict | None,
    *,
    max_symbols: int | None,
    source: str,
    as_of_date: str | None = None,
) -> dict:
    symbols = alert_symbols_from_payload(alert_payload)
    if source in ("background", "startup", "after_close"):
        symbols = _normalize_symbols(
            [
                *symbols,
                *get_open_entry_signal_symbols(),
                *_after_close_open_signal_discovery_symbols(source),
            ]
        )
    if not symbols:
        return {"status": "skipped", "reason": "no_alert_symbols", "symbols": []}

    full_enabled = _full_preload_enabled()
    if not full_enabled:
        return {"status": "skipped", "reason": "full_preload_disabled", "symbols": symbols}

    as_of_date = _normalize_as_of_date(as_of_date)
    pending = _symbols_needing_preload(symbols, as_of_date=as_of_date, full_only=True)
    if not pending:
        return {"status": "ready", "loaded": 0, "symbols": symbols}

    batch_size = _batch_size_for_pending(pending, max_symbols)
    if batch_size <= 0:
        return {"status": "skipped", "reason": "batch_size_zero", "symbols": pending}

    batch_symbols = pending[:batch_size]

    if os.getenv("ENTRY_DECISION_PRELOAD_INLINE", "").strip() == "1":
        if source == "alert":
            loaded_payloads, contexts, failed = _compute_interactive_artifacts(batch_symbols, as_of_date)
            return _store_worker_results(loaded_payloads, failed, as_of_date, source="alert", contexts=contexts)
        return _run_preload_inline(batch_symbols, as_of_date)

    _start_preload_worker(batch_symbols, as_of_date, source=source)
    return {"status": "started", "symbols": batch_symbols}


def preload_entry_decisions_from_alert_payload(
    alert_payload: dict | None,
    *,
    max_symbols: int | None = None,
    min_idle_seconds: float | None = 0,
    respect_market_gate: bool = False,
) -> dict:
    timeout_seconds = DEFAULT_WORKER_TIMEOUT_SECONDS
    if not _preload_lock.acquire(blocking=False):
        return {"status": "skipped", "reason": "preload_coordinator_busy"}

    try:
        market_gate = (
            _auto_preload_market_gate()
            if respect_market_gate
            else {"allowed": True, "reason": "market_gate_bypassed"}
        )
        terminated_worker = None
        if respect_market_gate and not market_gate.get("allowed", False):
            terminated_worker = _terminate_noninteractive_worker_for_market_gate(
                market_gate.get("reason", "market_closed")
            )

        worker_result = _reap_preload_worker(timeout_seconds)
        if not market_gate.get("allowed", False):
            return _auto_preload_skipped_result(
                market_gate,
                worker_result=worker_result,
                terminated_worker=terminated_worker,
            )

        symbols = alert_symbols_from_payload(alert_payload)
        if not symbols:
            if worker_result is not None:
                return worker_result
            return {"status": "skipped", "reason": "no_alert_symbols", "symbols": []}

        if not _full_preload_enabled():
            return {"status": "skipped", "reason": "full_preload_disabled", "symbols": symbols}

        as_of_date = _alert_payload_as_of_date(alert_payload)
        queued_symbols = _queue_alert_preload_symbols(symbols, as_of_date, front=True)
        if isinstance(worker_result, dict) and worker_result.get("status") == "running":
            return {
                **worker_result,
                "queued_symbols": queued_symbols,
                "queued_count": len(queued_symbols),
            }

        if not is_backend_idle(min_idle_seconds):
            return {
                "status": "skipped",
                "reason": "backend_busy",
                "queued_symbols": queued_symbols,
                "queued_count": len(queued_symbols),
            }

        # User-visible alert tickers are high intent. Keep every visible ticker
        # queued, then start the next alert batch without blocking this request.
        result = _start_next_queued_alert_preload(max_symbols=max_symbols)
        if result is not None:
            result["queued_symbols"] = queued_symbols
            result["queued_count"] = len(queued_symbols)
            if worker_result is not None:
                result["previous_worker"] = worker_result
            return result

        if worker_result is not None:
            return worker_result
        return {"status": "ready", "loaded": 0, "symbols": symbols}
    finally:
        _preload_lock.release()


def _reset_entry_decision_preload_state_for_tests() -> None:
    global _active_requests, _last_request_activity_at, _global_retry_after
    _terminate_worker()
    with _state_lock:
        _active_requests = 0
        _last_request_activity_at = 0.0
        _global_retry_after = 0.0
        _payload_cache.clear()
        _context_cache.clear()
        _failure_retry_after.clear()
        _interactive_failure_retry_after.clear()
        _alert_preload_queue.clear()
        _alert_preload_queued.clear()
