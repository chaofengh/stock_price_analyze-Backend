# tasks/daily_scan_tasks.py
from __future__ import annotations

import copy
import logging
import threading
from datetime import date, datetime, timedelta
from functools import lru_cache

import pandas_market_calendars as mcal
import pytz

from analysis.daily_scan import daily_scan

logger = logging.getLogger(__name__)

scan_updated_evt = threading.Event()   # signals SSE when a new scan is ready

# Scheduling policy (keep in sync with app.py cron)
_CHICAGO_TZ = pytz.timezone("America/Chicago")
_NYSE = mcal.get_calendar("NYSE")
_RUN_MINUTE = 35               # xx:35 CT every trading-hour slot
_MAX_LOOKAHEAD_DAYS = 14

_cache_lock = threading.Lock()
_cached_scan_result: dict | None = None
_last_successful_slot_key: str | None = None


def _now_chi() -> datetime:
    return datetime.now(_CHICAGO_TZ)


@lru_cache(maxsize=256)
def _session_bounds_for_date(date_key: str) -> tuple[datetime, datetime] | None:
    schedule = _NYSE.schedule(start_date=date_key, end_date=date_key)
    if schedule.empty:
        return None

    session = schedule.iloc[0]
    market_open = session.get("market_open")
    market_close = session.get("market_close")
    if market_open is None or market_close is None:
        return None

    open_chi = market_open.tz_convert(_CHICAGO_TZ).to_pydatetime()
    close_chi = market_close.tz_convert(_CHICAGO_TZ).to_pydatetime()
    return open_chi, close_chi


def _session_bounds_for_day(day: date) -> tuple[datetime, datetime] | None:
    return _session_bounds_for_date(day.isoformat())


def _is_within_regular_session(now: datetime) -> bool:
    bounds = _session_bounds_for_day(now.date())
    if bounds is None:
        return False
    open_dt, close_dt = bounds
    return open_dt <= now < close_dt


def _slot_key(now: datetime) -> str:
    return now.strftime("%Y-%m-%d %H")


def _next_hourly_slot_at_or_after(point: datetime) -> datetime:
    candidate = point.replace(minute=_RUN_MINUTE, second=0, microsecond=0)
    if candidate <= point:
        candidate += timedelta(hours=1)
    return candidate


def _next_run_time_chi(now: datetime | None = None) -> datetime:
    """
    Next NYSE-trading hourly slot in Chicago time.
    Slots are xx:35, during regular market session only.
    """
    now = now or _now_chi()

    for day_offset in range(_MAX_LOOKAHEAD_DAYS + 1):
        day = (now + timedelta(days=day_offset)).date()
        bounds = _session_bounds_for_day(day)
        if bounds is None:
            continue

        open_dt, close_dt = bounds
        start = max(now, open_dt) if day_offset == 0 else open_dt
        candidate = _next_hourly_slot_at_or_after(start)

        if candidate < close_dt:
            return candidate

    # Fallback to keep metadata stable even if calendar lookup fails unexpectedly.
    fallback = _next_hourly_slot_at_or_after(now)
    return fallback


def _with_meta(payload: dict, now: datetime | None = None, is_official: bool | None = None) -> dict:
    now = now or _now_chi()
    response = copy.deepcopy(payload) if isinstance(payload, dict) else {}
    response.setdefault("timestamp", now.strftime("%Y-%m-%d %H:%M:%S"))
    if not isinstance(response.get("alerts"), list):
        response["alerts"] = []

    meta = response.get("meta", {})
    if not isinstance(meta, dict):
        meta = {}
    meta["next_run_at"] = _next_run_time_chi(now).strftime("%Y-%m-%d %H:%M:%S")
    if is_official is not None:
        meta["is_official"] = bool(is_official)
    elif "is_official" not in meta:
        meta["is_official"] = False

    response["meta"] = meta
    return response


def _empty_payload(now: datetime | None = None) -> dict:
    now = now or _now_chi()
    return _with_meta(
        {
            "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
            "alerts": [],
        },
        now=now,
        is_official=False,
    )


def _get_cached_result() -> dict | None:
    with _cache_lock:
        if _cached_scan_result is None:
            return None
        return copy.deepcopy(_cached_scan_result)


def _store_cached_result(payload: dict, slot_key: str | None = None) -> None:
    global _cached_scan_result, _last_successful_slot_key
    with _cache_lock:
        _cached_scan_result = copy.deepcopy(payload)
        if slot_key is not None:
            _last_successful_slot_key = slot_key


def _get_last_successful_slot_key() -> str | None:
    with _cache_lock:
        return _last_successful_slot_key


def _run_scan_and_cache(
    *,
    now: datetime | None = None,
    is_official: bool,
    slot_key: str | None = None,
) -> dict:
    now = now or _now_chi()
    payload = _with_meta(daily_scan(), now=now, is_official=is_official)
    _store_cached_result(payload, slot_key=slot_key)
    return payload


def get_latest_scan_result(
    force: bool = False,
    allow_refresh_if_due: bool = True,
) -> dict:
    """
    Return latest scan result from in-memory cache.
    Compute a fresh scan only when forced or cache is empty.
    """
    del allow_refresh_if_due  # preserved for backward compatibility
    now = _now_chi()
    cached = _get_cached_result()

    if not force and cached is not None:
        return _with_meta(cached, now=now)

    is_official = _is_within_regular_session(now)
    try:
        return _run_scan_and_cache(now=now, is_official=is_official, slot_key=None)
    except Exception:
        logger.exception("Scan refresh failed; returning last known snapshot.")
        cached = _get_cached_result()
        if cached is not None:
            return _with_meta(cached, now=now)
        return _empty_payload(now=now)


def prime_scan_cache() -> dict:
    """
    One-time cache warm-up for startup.
    """
    return get_latest_scan_result(force=False, allow_refresh_if_due=False)


def daily_scan_wrapper():
    """
    Hourly scheduler wrapper:
    - skip when market session is closed
    - run at most once per hour slot
    - signal SSE listeners after successful refresh
    """
    now = _now_chi()

    if not _is_within_regular_session(now):
        return {
            "status": "skipped",
            "reason": "market_closed",
            "next_run_at": _next_run_time_chi(now).strftime("%Y-%m-%d %H:%M:%S"),
        }

    current_slot = _slot_key(now)
    if _get_last_successful_slot_key() == current_slot:
        return {
            "status": "skipped",
            "reason": "already_scanned_this_hour",
            "slot": current_slot,
            "next_run_at": _next_run_time_chi(now).strftime("%Y-%m-%d %H:%M:%S"),
        }

    try:
        result = _run_scan_and_cache(now=now, is_official=True, slot_key=current_slot)
    except Exception:
        logger.exception("Hourly scan failed for slot %s.", current_slot)
        return {
            "status": "error",
            "reason": "scan_failed",
            "next_run_at": _next_run_time_chi(now).strftime("%Y-%m-%d %H:%M:%S"),
        }

    scan_updated_evt.set()
    return result


def _reset_scan_state_for_tests() -> None:
    global _cached_scan_result, _last_successful_slot_key
    with _cache_lock:
        _cached_scan_result = None
        _last_successful_slot_key = None
    scan_updated_evt.clear()
    _session_bounds_for_date.cache_clear()
