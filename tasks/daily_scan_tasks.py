# tasks/daily_scan_tasks.py
from __future__ import annotations
import threading
from datetime import datetime, time as dtime, timedelta
import pytz

from analysis.daily_scan import daily_scan

scan_updated_evt = threading.Event()   # signals SSE when a new scan is ready

# Scheduling policy (keep in sync with app.py cron)
_CHICAGO_TZ = pytz.timezone("America/Chicago")
_RUN_TIME = dtime(hour=15, minute=31)       # 16:02 CT
_RUN_WEEKDAYS = {0, 1, 2, 3, 4}            # Mon=0 ... Fri=4


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
    day = now
    if day.weekday() in _RUN_WEEKDAYS and day.time() < _RUN_TIME:
        target = day
    else:
        while True:
            day = day + timedelta(days=1)
            if day.weekday() in _RUN_WEEKDAYS:
                target = day
                break
    naive = datetime(target.year, target.month, target.day, _RUN_TIME.hour, _RUN_TIME.minute, 0)
    return _CHICAGO_TZ.localize(naive)


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
    Compute and return the latest daily scan result.
    """
    now = _now_chi()
    result = daily_scan()
    return _with_meta(result, now, is_official=_is_past_run_window(now))


def daily_scan_wrapper():
    """Force-run daily_scan and signal SSE listeners."""
    result = get_latest_scan_result(force=True)
    scan_updated_evt.set()
    return result
