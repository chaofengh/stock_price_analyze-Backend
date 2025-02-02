from flask import Flask, request, jsonify
from flask_cors import CORS
from analysis.get_summary import get_summary

from dotenv import load_dotenv

# Load environment variables from a .env file if present
load_dotenv()

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
        summary = get_summary(symbol)
        return jsonify(summary), 200
    except Exception as e:
        # You can customize error handling here
        return jsonify({'error': str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
