# alerts_routes.py
import json
import time
from datetime import datetime, timezone

from flask import Blueprint, Response, request

import tasks.daily_scan_tasks as daily_scan_tasks
from database.ticker_repository import get_all_tickers

alerts_blueprint = Blueprint("alerts", __name__)


@alerts_blueprint.route("/api/alerts/stream", methods=["GET"])
def alerts_stream():

    # --- Parse & validate optional user_id ----------------------------------
    user_id_param = request.args.get("user_id")
    try:
        user_id = int(user_id_param) if user_id_param else None
    except ValueError:
        user_id = None

    def event_stream():
        last_sent_date = None  # Track the day we've already pushed to client

        while True:
            # -----------------------------------------------------------------
            # 1) Ensure we have a scan for *today* (in UTC) --------------------
            # -----------------------------------------------------------------
            today = datetime.now(timezone.utc).date().isoformat()  # "YYYY‑MM‑DD"

            latest = daily_scan_tasks.latest_scan_result
            latest_day = (
                latest["timestamp"].split(" ")[0]
                if latest and "timestamp" in latest
                else None
            )

            if latest_day != today:
                # Either no scan yet or it's stale → run a fresh daily scan.
                # daily_scan() updates latest_scan_result in‑place.
                daily_scan_tasks.daily_scan_wrapper()
                latest = daily_scan_tasks.latest_scan_result
                latest_day = (
                    latest["timestamp"].split(" ")[0]
                    if latest and "timestamp" in latest
                    else None
                )

            if latest and latest_day != last_sent_date:
                payload = latest.copy()

                # Filter by the user's watch‑list if a user_id was provided
                if user_id is not None:
                    watchlist = get_all_tickers(user_id=user_id)
                    payload["alerts"] = [
                        alert
                        for alert in payload.get("alerts", [])
                        if alert.get("ticker") in watchlist
                    ]

                yield f"data: {json.dumps(payload)}\n\n"  # SSE format
                last_sent_date = latest_day

            time.sleep(3600)  # Adjust if you need finer granularity

    return Response(event_stream(), mimetype="text/event-stream")
