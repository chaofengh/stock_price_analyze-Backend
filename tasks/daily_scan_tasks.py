# tasks/daily_scan_tasks.py
import threading
from datetime import datetime, time as dtime
import pytz

from analysis.daily_scan import daily_scan

# Module-level cache + lock
latest_scan_result = None
_result_lock = threading.Lock()

# Scheduling policy (keep in sync with app.py cron)
_CHICAGO_TZ = pytz.timezone("America/Chicago")
_RUN_TIME = dtime(hour=16, minute=2)  # 16:02 CT
_RUN_WEEKDAYS = {0, 1, 2, 3, 4}      # Mon=0 ... Fri=4

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
    """Return True if it's a weekday and time is >= scheduled run time."""
    if now.weekday() not in _RUN_WEEKDAYS:
        return False
    return now.time() >= _RUN_TIME

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
    - Else: return whatever we have (stale cache) WITHOUT recomputing.
      (Prevents midnight / first-visit recomputes.)
    """
    global latest_scan_result

    if not force:
        cached_date = _result_date_str(latest_scan_result)
        if cached_date == _today_str_chicago() and latest_scan_result is not None:
            return latest_scan_result

    with _result_lock:
        if not force:
            cached_date = _result_date_str(latest_scan_result)
            if cached_date == _today_str_chicago() and latest_scan_result is not None:
                return latest_scan_result

            if allow_refresh_if_due and _is_past_run_window(_now_chi()):
                # It's time (or later) to produce today's scan -> recompute now.
                latest_scan_result = daily_scan()
                return latest_scan_result

            # Not time yet. If we have something cached, return it.
            if latest_scan_result is not None:
                return latest_scan_result

            # Nothing cached (e.g., first boot). Produce one so UI has data.
            latest_scan_result = daily_scan()
            return latest_scan_result

        # force=True
        latest_scan_result = daily_scan()
        return latest_scan_result

# Backwards-compat function name you already used elsewhere
def daily_scan_wrapper():
    """
    Force-run daily_scan and update the cache. Used by the scheduler.
    """
    return get_latest_scan_result(force=True)
