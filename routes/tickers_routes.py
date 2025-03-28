#tickers_routes.py
from flask import Blueprint, jsonify, request
from database.ticker_repository import get_all_tickers,add_ticker_to_user_list, remove_ticker_from_user_list
from analysis.data_preparation import fetch_stock_data

tickers_blueprint = Blueprint('tickers', __name__)

@tickers_blueprint.route('/api/tickers', methods=['GET'])
def get_tickers():
    """
    Returns intraday data for tickers.
    
    If a 'user_id' query parameter is provided (e.g., ?user_id=123), 
    only the tickers in that user's default list are returned.
    Otherwise, it returns data for all tickers in the global table.
    
    The response JSON is structured as:
      {
        "TSLA": [{...}, {...}, ...],
        "PLTR": [{...}, {...}, ...],
        ...
      }
    where each key is a ticker symbol and the value is a list of dictionaries 
    (each representing a row from the intraday data DataFrame).
    """
    try:
        # Get user_id from query parameters if available
        user_id = request.args.get('user_id')
        if user_id:
            tickers = get_all_tickers(user_id=int(user_id))
        else:
            tickers = get_all_tickers()

        # For intraday price data, use a 1-day period and 15-minute interval.
        intraday_data = fetch_stock_data(tickers, period="1d", interval="15m")

        # Serialize the data: convert each DataFrame to a list of dictionaries.
        serialized_data = {}
        for ticker, df in intraday_data.items():
            serialized_data[ticker] = df.to_dict(orient='records')

        return jsonify(serialized_data), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500



@tickers_blueprint.route('/api/tickers', methods=['POST'])
def add_ticker():
    """
    Expects JSON:
      { "user_id": 123, "ticker": "TSLA" }
    or 
      { "user_id": 123, "tickers": ["TSLA", "AAPL"] }
    """
    try:
        data = request.get_json()
        user_id = data.get("user_id")
        if not user_id:
            return jsonify({'error': 'user_id is required'}), 400

        # Instead of inserting globally, we add the ticker(s) to the user’s list.
        # Example: add_ticker_to_user_list(user_id, ticker)
        if "ticker" in data:
            add_ticker_to_user_list(user_id, data["ticker"])
        elif "tickers" in data:
            for t in data["tickers"]:
                add_ticker_to_user_list(user_id, t)
        else:
            return jsonify({'error': 'ticker or tickers field is required'}), 400

        return jsonify({'status': 'success'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@tickers_blueprint.route('/api/tickers', methods=['DELETE'])
def delete_ticker():
    """
    Expects JSON:
      { "user_id": 123, "ticker": "TSLA" }
    """
    try:
        data = request.get_json()
        user_id = data.get("user_id")
        ticker = data.get("ticker")

        if not user_id or not ticker:
            return jsonify({'error': 'user_id and ticker are required'}), 400

        # Remove ticker from the user’s watchlist only
        remove_ticker_from_user_list(user_id, ticker)
        return jsonify({'status': 'deleted'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500