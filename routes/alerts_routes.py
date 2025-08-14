# routes/alerts_routes.py
import json
import time
from flask import Blueprint, Response, jsonify, request

from tasks.daily_scan_tasks import get_latest_scan_result
from database.ticker_repository import get_all_tickers

alerts_blueprint = Blueprint('alerts', __name__)

def _filter_for_user(result: dict, user_id: int | None) -> dict:
    """Return a shallow-copied result with alerts filtered by the user's watchlist (if user_id provided)."""
    if user_id is None:
        return result
    try:
        watchlist = set(get_all_tickers(user_id=user_id) or [])
    except Exception:
        # Fail open: if we can't load the watchlist, return unfiltered to avoid 500s.
        return result

    filtered = result.copy()
    filtered_alerts = [
        a for a in (result.get("alerts") or [])
        if a.get("ticker") in watchlist
    ]
    filtered["alerts"] = filtered_alerts
    return filtered

@alerts_blueprint.route('/api/alerts/latest', methods=['GET'])
def alerts_latest():
    """
    JSON endpoint:
      - Returns the cached result.
      - If it's past today's run window (>= 16:02 CT on a weekday), a fresh
        computation will occur transparently before returning.
      - Importantly: will NOT recompute at midnight just because the date changed.
    """
    user_id = request.args.get("user_id")
    try:
        user_id = int(user_id) if user_id is not None else None
    except ValueError:
        user_id = None

    result = get_latest_scan_result(allow_refresh_if_due=True)
    result = _filter_for_user(result, user_id)
    return jsonify(result), 200

@alerts_blueprint.route('/api/alerts/stream', methods=['GET'])
def alerts_stream():
    """
    SSE endpoint:
      - Sends the current cached result immediately on connect.
      - Then checks frequently and sends again when the *timestamp* changes.
      - Emits keep-alive heartbeats to avoid proxy disconnects.
    """
    user_id = request.args.get("user_id")
    try:
        user_id = int(user_id) if user_id is not None else None
    except ValueError:
        user_id = None

    def event_stream():
        last_sent_ts = None
        idle = 0
        while True:
            # This will compute only if it's past the run window; otherwise returns cache.
            result = get_latest_scan_result(allow_refresh_if_due=True)
            ts = result.get("timestamp") or ""

            if ts and ts != last_sent_ts:
                payload = _filter_for_user(result, user_id)
                yield "event: alerts_update\n"
                yield f"data: {json.dumps(payload)}\n\n"
                last_sent_ts = ts
                idle = 0
            else:
                # Keep-alive every 30s
                if idle % 30 == 0:
                    yield f": heartbeat {int(time.time())}\n\n"

            time.sleep(60*60)
            idle += 1

    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
        "Connection": "keep-alive",
    }
    return Response(event_stream(), mimetype='text/event-stream', headers=headers)
