import json
import time
from flask import Blueprint, Response, jsonify

# Import the latest_scan_result global and function
from tasks.daily_scan_tasks import latest_scan_result

alerts_blueprint = Blueprint('alerts', __name__)

@alerts_blueprint.route('/api/alerts/stream', methods=['GET'])
def alerts_stream():
    """
    SSE endpoint. Streams the latest scan result whenever it changes.
    """
    def event_stream():
        last_known_timestamp = None
        while True:
            if latest_scan_result:
                current_timestamp = latest_scan_result["timestamp"]
                if current_timestamp != last_known_timestamp:
                    data_str = json.dumps(latest_scan_result)
                    yield f"data: {data_str}\n\n"
                    last_known_timestamp = current_timestamp
            time.sleep(1800)
    return Response(event_stream(), mimetype='text/event-stream')

@alerts_blueprint.route('/api/alerts/latest', methods=['GET'])
def get_latest_alerts():
    if latest_scan_result:
        return jsonify(latest_scan_result)
    else:
        return jsonify({"timestamp": None, "alerts": []}), 200
