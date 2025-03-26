#alerts_routes.py
import json
import time
from flask import Blueprint, Response, jsonify
import tasks.daily_scan_tasks as daily_scan_tasks

alerts_blueprint = Blueprint('alerts', __name__)

@alerts_blueprint.route('/api/alerts/stream', methods=['GET'])
def alerts_stream():
    def event_stream():
        last_known_timestamp = None
        while True:
            if daily_scan_tasks.latest_scan_result:
                current_timestamp = daily_scan_tasks.latest_scan_result["timestamp"]
                if current_timestamp != last_known_timestamp:
                    data_str = json.dumps(daily_scan_tasks.latest_scan_result)
                    yield f"data: {data_str}\n\n"
                    last_known_timestamp = current_timestamp
            time.sleep(1800)
    return Response(event_stream(), mimetype='text/event-stream')

@alerts_blueprint.route('/api/alerts/latest', methods=['GET'])
def get_latest_alerts():
    if daily_scan_tasks.latest_scan_result:
        return jsonify(daily_scan_tasks.latest_scan_result)
    else:
        return jsonify({"timestamp": None, "alerts": []}), 200
