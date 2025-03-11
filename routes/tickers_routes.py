from flask import Blueprint, jsonify, request
from database.ticker_repository import get_all_tickers, insert_tickers, remove_ticker
from analysis.data_preparation import fetch_stock_data

tickers_blueprint = Blueprint('tickers', __name__)

@tickers_blueprint.route('/api/tickers', methods=['DELETE'])
def delete_ticker():
    """
    Expects a JSON payload:
      {"ticker": "TSLA"}
    """
    try:
        data = request.get_json()
        if not data or "ticker" not in data:
            return jsonify({'error': 'ticker field is required'}), 400

        remove_ticker(data["ticker"])
        return jsonify({'status': 'deleted'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@tickers_blueprint.route('/api/tickers', methods=['GET'])
def get_tickers():
    """
    Returns intraday data for all tickers in the database.
    """
    try:
        tickers = get_all_tickers()
        # For intraday price movement, we use period="1d" and interval="15m"
        intraday_data = fetch_stock_data(tickers, period="1d", interval="15m")
        
        serialized_data = {}
        for ticker, df in intraday_data.items():
            serialized_data[ticker] = df.to_dict(orient='records')
        
        return jsonify(serialized_data), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@tickers_blueprint.route('/api/tickers', methods=['POST'])
def add_ticker():
    """
    Expects JSON payload in one of the following formats:
      {"ticker": "TSLA"}
    or
      {"tickers": ["TSLA", "AAPL"]}
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No input data provided'}), 400

        # Insert single ticker
        if "ticker" in data:
            insert_tickers([data["ticker"]])
        # Insert multiple tickers
        elif "tickers" in data:
            insert_tickers(data["tickers"])
        else:
            return jsonify({'error': 'Invalid input data. Use "ticker" or "tickers" field.'}), 400

        return jsonify({'status': 'success'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
