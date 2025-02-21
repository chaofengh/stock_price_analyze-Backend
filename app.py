from flask import Flask, request, jsonify
from flask_cors import CORS
from analysis.summary import get_summary
from dotenv import load_dotenv
import numpy as np
import pandas as pd

load_dotenv()

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

# Load environment variables from a .env file if present


app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "http://localhost:3000"}})

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

if __name__ == "__main__":
    app.run(debug=True)
