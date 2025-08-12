import threading
from datetime import datetime
import pytz

from analysis.daily_scan import daily_scan

# Module-level cache + lock
latest_scan_result = None
_CHICAGO_TZ = pytz.timezone("America/Chicago")
_result_lock = threading.Lock()

def _today_str_chicago() -> str:
    return datetime.now(_CHICAGO_TZ).strftime("%Y-%m-%d")

def _result_date_str(result: dict | None) -> str | None:
    if not result:
        return None
    ts = result.get("timestamp")
    if not ts:
        return None
    return ts[:10]  # 'YYYY-MM-DD' from 'YYYY-MM-DD HH:MM:SS'

def get_latest_scan_result(force: bool = False) -> dict:
    """
    Return the latest daily scan result.
    - If 'force' is True: recompute and update cache.
    - Otherwise: recompute only if cached result is missing or not from today's Chicago date.
    """
    global latest_scan_result

    if not force:
        cached_date = _result_date_str(latest_scan_result)
        if cached_date == _today_str_chicago() and latest_scan_result is not None:
            return latest_scan_result

    # Double-checked locking to avoid redundant scans under concurrency
    with _result_lock:
        if not force:
            cached_date = _result_date_str(latest_scan_result)
            if cached_date == _today_str_chicago() and latest_scan_result is not None:
                return latest_scan_result

        latest_scan_result = daily_scan()
        return latest_scan_result

# Backwards-compat function name you already used elsewhere
def daily_scan_wrapper():
    """
    Force-run daily_scan and update the cache.
    """
    return get_latest_scan_result(force=True)
