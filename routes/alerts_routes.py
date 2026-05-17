# routes/alerts_routes.py
from __future__ import annotations
import json
import time
from flask import Blueprint, Response, jsonify, request

from tasks.daily_scan_tasks import get_latest_scan_result, scan_updated_evt
from tasks.entry_decision_preload_tasks import preload_entry_decisions_from_alert_payload
from tasks.entry_signal_tasks import list_open_entry_signals
from tasks.entry_signal_story_tasks import build_open_entry_signal_stories
from database.ticker_repository import get_all_tickers
from utils.auth import AuthError, authenticate_bearer_token

alerts_blueprint = Blueprint('alerts', __name__)


def _payload_signature(payload: dict) -> str:
    return json.dumps(
        {
            "timestamp": payload.get("timestamp") or "",
            "alerts": payload.get("alerts") or [],
            "open_entry_signals": payload.get("open_entry_signals") or [],
            "open_entry_signal_stories": payload.get("open_entry_signal_stories") or [],
        },
        sort_keys=True,
        default=str,
    )


def _filter_for_user(result: dict, user_id: int | None) -> dict:
    if user_id is None:
        filtered = result.copy()
        filtered["alerts"] = []
        filtered["open_entry_signals"] = []
        filtered["open_entry_signal_stories"] = []
        return filtered
    try:
        watchlist = {t.upper() for t in (get_all_tickers(user_id=user_id) or []) if t}
    except Exception:
        filtered = result.copy()
        filtered["alerts"] = []
        filtered["open_entry_signals"] = []
        filtered["open_entry_signal_stories"] = []
        return filtered  # fail closed

    def _alert_symbol(alert: dict) -> str | None:
        symbol = alert.get("symbol") or alert.get("ticker")
        return symbol.upper() if isinstance(symbol, str) else None

    filtered = result.copy()
    filtered["alerts"] = [
        a for a in (result.get("alerts") or [])
        if _alert_symbol(a) in watchlist
    ]
    open_entry_signals = list_open_entry_signals(symbols=watchlist)
    filtered["open_entry_signals"] = open_entry_signals
    filtered["open_entry_signal_stories"] = build_open_entry_signal_stories(open_entry_signals)
    return filtered


def _prepare_alert_payload_for_user(result: dict, user_id: int | None) -> dict:
    """
    Build the user-visible alert payload and opportunistically refresh Entry
    Decision signal state before the final open-signal list is attached.
    """
    filtered = _filter_for_user(result, user_id)
    preload_result = preload_entry_decisions_from_alert_payload(filtered)
    refreshed = _filter_for_user(result, user_id)
    refreshed["entry_decision_preload"] = preload_result
    return refreshed

@alerts_blueprint.route('/api/alerts/latest', methods=['GET'])
def alerts_latest():
    """
    Returns the latest scan result.
    """
    try:
        auth = authenticate_bearer_token(request.headers.get("Authorization"))
    except AuthError as e:
        return jsonify({"error": str(e)}), 401

    result = _prepare_alert_payload_for_user(
        get_latest_scan_result(allow_refresh_if_due=True),
        auth.user_id,
    )
    return jsonify(result), 200

@alerts_blueprint.route('/api/alerts/stream', methods=['GET'])
def alerts_stream():
    """
    SSE endpoint (event-driven, low CPU).
    """
    try:
        auth = authenticate_bearer_token(request.headers.get("Authorization"))
    except AuthError as e:
        return jsonify({"error": str(e)}), 401

    def event_stream():
        cur = _prepare_alert_payload_for_user(
            get_latest_scan_result(allow_refresh_if_due=True),
            auth.user_id,
        )
        last_signature = _payload_signature(cur)
        yield "event: alerts_update\n"
        yield f"data: {json.dumps(cur)}\n\n"

        while True:
            fired = scan_updated_evt.wait(timeout=30)
            if not fired:
                heartbeat_payload = _prepare_alert_payload_for_user(
                    get_latest_scan_result(allow_refresh_if_due=False),
                    auth.user_id,
                )
                signature = _payload_signature(heartbeat_payload)
                if signature != last_signature:
                    yield "event: alerts_update\n"
                    yield f"data: {json.dumps(heartbeat_payload)}\n\n"
                    last_signature = signature
                    continue
                yield f": heartbeat {int(time.time())}\n\n"
                continue

            scan_updated_evt.clear()
            payload = _prepare_alert_payload_for_user(
                get_latest_scan_result(allow_refresh_if_due=False), auth.user_id
            )
            signature = _payload_signature(payload)
            if signature != last_signature:
                yield "event: alerts_update\n"
                yield f"data: {json.dumps(payload)}\n\n"
                last_signature = signature

    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
        "Connection": "keep-alive",
    }
    return Response(event_stream(), mimetype='text/event-stream', headers=headers)
