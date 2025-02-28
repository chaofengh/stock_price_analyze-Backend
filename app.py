# app.py
import os
from flask import Flask, Response, jsonify,request
from flask_cors import CORS
import json
import atexit
from apscheduler.schedulers.background import BackgroundScheduler
from analysis.summary import get_summary
from dotenv import load_dotenv
import numpy as np
import pandas as pd
from analysis.daily_scan import daily_scan

load_dotenv()
frontend_origin = os.getenv('front_end_client_website')

def convert_to_python_types(obj):
    """
    Recursively convert NumPy and Pandas-specific objects
    to native Python types for JSON serialization.
    """
    if isinstance(obj, dict):
        return {k: convert_to_python_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_python_types(item) for item in obj]
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, pd.Timestamp):
        # Convert Timestamps to string (ISO 8601 format, for example)
        return obj.isoformat()
    else:
        return obj

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": frontend_origin}})

@app.route('/api/summary', methods=['GET'])
def summary_endpoint():
    """
    Example usage:
      GET /api/summary?symbol=QQQ
    Returns the analysis summary as JSON.
    """
    symbol = request.args.get('symbol', default='QQQ')
    try:
        df_summary = get_summary(symbol)
        df_summary = convert_to_python_types(df_summary)
        return jsonify(df_summary), 200
    except Exception as e:
        # You can customize error handling here
        return jsonify({'error': str(e)}), 500

# This variable holds the most recent scan result.
# It gets overwritten every time daily_scan runs.
latest_scan_result = None
print('asdasdasdasd')
def daily_scan_wrapper():
    """
    Run daily_scan and store the result for broadcasting.
    """
    global latest_scan_result
    latest_scan_result = daily_scan()
    print("daily_scan ran, updated latest_scan_result.")

@app.route('/api/alerts/stream', methods=['GET'])
def alerts_stream():
    """
    SSE endpoint. Streams the latest scan result whenever it changes.
    For demonstration, we yield the current 'latest_scan_result' 
    whenever a new result arrives.
    """
    def event_stream():
        # We'll track the last known timestamp to detect new data.
        last_known_timestamp = None

        while True:
            if latest_scan_result:
                current_timestamp = latest_scan_result["timestamp"]
                # If there's a new scan result, push it down the SSE stream.
                if current_timestamp != last_known_timestamp:
                    data_str = json.dumps(latest_scan_result)
                    yield f"data: {data_str}\n\n"
                    last_known_timestamp = current_timestamp

    return Response(event_stream(), mimetype='text/event-stream')

# A quick API to fetch the latest result if needed
@app.route('/api/alerts/latest', methods=['GET'])
def get_latest_alerts():
    if latest_scan_result:
        return jsonify(latest_scan_result)
    else:
        return jsonify({"timestamp": None, "alerts": []}), 200

# Scheduler to run daily_scan at 4PM every day
scheduler = BackgroundScheduler()
scheduler.add_job(daily_scan_wrapper, 'cron', hour=16, minute=0)
scheduler.start()

atexit.register(lambda: scheduler.shutdown(wait=False))

if __name__ == "__main__":
    # For dev testing, run the schedule job once on startup
    daily_scan_wrapper()

    # Run the app
    app.run(debug=True)

