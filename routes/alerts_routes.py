#alerts_routes.py
import json
import time
from flask import Blueprint, Response, jsonify, request
import tasks.daily_scan_tasks as daily_scan_tasks
from database.ticker_repository import get_all_tickers

alerts_blueprint = Blueprint('alerts', __name__)

@alerts_blueprint.route('/api/alerts/stream', methods=['GET'])
def alerts_stream():
    # Get the optional user_id from query parameters
    user_id = request.args.get("user_id")
    if user_id:
        try:
            user_id = int(user_id)
        except ValueError:
            user_id = None

    def event_stream():
        last_known_timestamp = None
        while True:
            if daily_scan_tasks.latest_scan_result:
                current_timestamp = daily_scan_tasks.latest_scan_result["timestamp"]
                if current_timestamp != last_known_timestamp:
                    # Start with the global scan result.
                    result = daily_scan_tasks.latest_scan_result.copy()
                    
                    # If user_id was provided, filter alerts based on the user's watchlist.
                    if user_id is not None:
                        # Get the tickers on the user's default list.
                        user_watchlist = get_all_tickers(user_id=user_id)
                        filtered_alerts = [
                            alert for alert in result.get("alerts", [])
                            if alert.get("ticker") in user_watchlist
                        ]
                        result["alerts"] = filtered_alerts

                    data_str = json.dumps(result)
                    yield f"data: {data_str}\n\n"
                    last_known_timestamp = current_timestamp
            time.sleep(1800)  # Sleep for 30 minutes between checks.
    return Response(event_stream(), mimetype='text/event-stream')


@alerts_blueprint.route('/api/alerts/latest', methods=['GET'])
def get_latest_alerts():
    if daily_scan_tasks.latest_scan_result:
        return jsonify(daily_scan_tasks.latest_scan_result)
    else:
        return jsonify({"timestamp": None, "alerts": []}), 200
