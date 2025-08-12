import json
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
    Simple JSON endpoint:
      - If cache is missing/stale (not today's date in America/Chicago), recompute.
      - Return the (possibly newly computed) result, optionally filtered by user_id watchlist.
    """
    user_id = request.args.get("user_id")
    try:
        user_id = int(user_id) if user_id is not None else None
    except ValueError:
        user_id = None

    result = get_latest_scan_result()  # will compute if stale
    result = _filter_for_user(result, user_id)
    return jsonify(result), 200

@alerts_blueprint.route('/api/alerts/stream', methods=['GET'])
def alerts_stream():
    """
    SSE endpoint:
      - Immediately sends the current (or freshly computed) result when the client connects.
      - Then checks once per hour and only sends again if the calendar day flips.
    """
    user_id = request.args.get("user_id")
    try:
        user_id = int(user_id) if user_id is not None else None
    except ValueError:
        user_id = None

    def event_stream():
        import time
        last_sent_date = None
        while True:
            result = get_latest_scan_result()  # compute if stale
            current_date = (result.get("timestamp") or "")[:10]

            # Send immediately on first loop, then only if the date changes.
            if current_date != last_sent_date:
                payload = _filter_for_user(result, user_id)
                data_str = json.dumps(payload)
                yield f"data: {data_str}\n\n"
                last_sent_date = current_date

            # Sleep an hour; adjust if you want tighter checks
            time.sleep(3600)

    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",  # helpful with some proxies
    }
    return Response(event_stream(), mimetype='text/event-stream', headers=headers)
