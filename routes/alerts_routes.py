# routes/alerts_routes.py
from __future__ import annotations
import json
import time
from flask import Blueprint, Response, jsonify, request

from tasks.daily_scan_tasks import (
    get_latest_scan_result,
    get_last_timestamp,
    scan_updated_evt,
)
from database.ticker_repository import get_all_tickers

alerts_blueprint = Blueprint('alerts', __name__)

def _filter_for_user(result: dict, user_id: int | None) -> dict:
    if user_id is None:
        return result
    try:
        watchlist = {t.upper() for t in (get_all_tickers(user_id=user_id) or []) if t}
    except Exception:
        return result  # fail open

    def _alert_symbol(alert: dict) -> str | None:
        symbol = alert.get("symbol") or alert.get("ticker")
        return symbol.upper() if isinstance(symbol, str) else None

    filtered = result.copy()
    filtered["alerts"] = [
        a for a in (result.get("alerts") or [])
        if _alert_symbol(a) in watchlist
    ]
    return filtered

@alerts_blueprint.route('/api/alerts/latest', methods=['GET'])
def alerts_latest():
    """
    Returns cached result.
    Recomputes only after 16:02 CT on weekdays (or when the scheduler runs).
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
    SSE endpoint (event-driven, low CPU).
    """
    user_id = request.args.get("user_id")
    try:
        user_id = int(user_id) if user_id is not None else None
    except ValueError:
        user_id = None

    def event_stream():
        cur = get_latest_scan_result(allow_refresh_if_due=True)
        cur = _filter_for_user(cur, user_id)
        last_ts = cur.get("timestamp") or ""
        yield "event: alerts_update\n"
        yield f"data: {json.dumps(cur)}\n\n"

        while True:
            fired = scan_updated_evt.wait(timeout=30)
            if not fired:
                yield f": heartbeat {int(time.time())}\n\n"
                continue

            scan_updated_evt.clear()
            ts = get_last_timestamp() or ""
            if ts and ts != last_ts:
                payload = _filter_for_user(
                    get_latest_scan_result(allow_refresh_if_due=False), user_id
                )
                yield "event: alerts_update\n"
                yield f"data: {json.dumps(payload)}\n\n"
                last_ts = ts

    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
        "Connection": "keep-alive",
    }
    return Response(event_stream(), mimetype='text/event-stream', headers=headers)
