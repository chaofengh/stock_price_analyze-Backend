# tasks/daily_scan_tasks.py
import threading
from datetime import datetime, time as dtime, timedelta
import pytz

from analysis.daily_scan import daily_scan

# Module-level cache + lock + update event
latest_scan_result = None
_result_lock = threading.Lock()
scan_updated_evt = threading.Event()   # signals SSE when a new scan is ready

# Scheduling policy (keep in sync with app.py cron)
_CHICAGO_TZ = pytz.timezone("America/Chicago")
_RUN_TIME = dtime(hour=15, minute=31)       # 16:02 CT
_RUN_WEEKDAYS = {0, 1, 2, 3, 4}            # Mon=0 ... Fri=4

def _today_str_chicago() -> str:
    return datetime.now(_CHICAGO_TZ).strftime("%Y-%m-%d")

def _result_date_str(result: dict | None) -> str | None:
    if not result:
        return None
    ts = result.get("timestamp")
    if not ts:
        return None
    return ts[:10]  # 'YYYY-MM-DD' from 'YYYY-MM-DD HH:MM:SS'

def _now_chi():
    return datetime.now(_CHICAGO_TZ)

def _is_past_run_window(now: datetime) -> bool:
    """True if it's a weekday and time is >= scheduled run time (Chicago)."""
    if now.weekday() not in _RUN_WEEKDAYS:
        return False
    return now.time() >= _RUN_TIME

def _next_run_time_chi(now: datetime | None = None) -> datetime:
    """Next 16:02 CT on a weekday, in America/Chicago tz."""
    now = now or _now_chi()
    # If it's a weekday and before today's run time, pick today; else advance.
    day = now
    if day.weekday() in _RUN_WEEKDAYS and day.time() < _RUN_TIME:
        target = day
    else:
        # jump to the next weekday in the set
        while True:
            day = day + timedelta(days=1)
            if day.weekday() in _RUN_WEEKDAYS:
                target = day
                break
    naive = datetime(target.year, target.month, target.day, _RUN_TIME.hour, _RUN_TIME.minute, 0)
    return _CHICAGO_TZ.localize(naive)

def get_last_timestamp() -> str | None:
    r = latest_scan_result
    return (r.get("timestamp") if r else None) if isinstance(r, dict) else None

def _with_meta(payload: dict, now: datetime | None = None, is_official: bool | None = None) -> dict:
    now = now or _now_chi()
    meta = payload.get("meta", {})
    meta["next_run_at"] = _next_run_time_chi(now).strftime("%Y-%m-%d %H:%M:%S")
    if is_official is not None:
        meta["is_official"] = bool(is_official)
    payload["meta"] = meta
    return payload

def get_latest_scan_result(
    force: bool = False,
    allow_refresh_if_due: bool = True,
) -> dict:
    """
    Return the latest daily scan result.

    - If `force`: recompute unconditionally (used by the APScheduler job).
    - Else if cache is from today (Chicago): return it.
    - Else if `allow_refresh_if_due` AND we are past today's run window (>= 16:02 CT on a weekday):
        recompute and return.
    - Else: DO NOT compute before 16:02. Return the latest cached payload (if any)
      and include meta['next_run_at']. If no cache exists, return a pending skeleton.
    """
    global latest_scan_result
    now = _now_chi()

    if not force:
        cached_date = _result_date_str(latest_scan_result)
        if cached_date == _today_str_chicago() and latest_scan_result is not None:
            return _with_meta(latest_scan_result.copy(), now, is_official=True)

    with _result_lock:
        if not force:
            cached_date = _result_date_str(latest_scan_result)
            if cached_date == _today_str_chicago() and latest_scan_result is not None:
                return _with_meta(latest_scan_result.copy(), now, is_official=True)

            if allow_refresh_if_due and _is_past_run_window(now):
                latest_scan_result = daily_scan()
                latest_scan_result = _with_meta(latest_scan_result, now, is_official=True)
                scan_updated_evt.set()
                return latest_scan_result

            if latest_scan_result is not None:
                # Serve yesterday's official result (or whatever is cached) and mark as not today's.
                return _with_meta(latest_scan_result.copy(), now, is_official=False)

            # No cache yet and not due -> do not compute early.
            pending = {
                "timestamp": None,
                "alerts": [],
            }
            pending = _with_meta(pending, now, is_official=False)
            return pending

        # force=True: the scheduler-driven official run
        latest_scan_result = daily_scan()
        latest_scan_result = _with_meta(latest_scan_result, now, is_official=True)
        scan_updated_evt.set()
        return latest_scan_result

def daily_scan_wrapper():
    """Force-run daily_scan and update the cache. Used by the scheduler."""
    return get_latest_scan_result(force=True)
