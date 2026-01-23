#tickers_routes.py
from flask import Blueprint, jsonify, request
from database.ticker_repository import (
    add_ticker_to_user_list,
    get_all_tickers,
    get_price_movement_data,
    remove_ticker_from_user_list,
    replace_user_watchlist,
)
from utils.auth import AuthError, authenticate_bearer_token

tickers_blueprint = Blueprint('tickers', __name__)

@tickers_blueprint.route('/api/tickers', methods=['GET'])
def get_tickers():
    """
    Returns intraday data for tickers.
    
    Requires an `Authorization: Bearer <token>` header and returns the
    tickers in the authenticated user's default list.
    
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
        auth = authenticate_bearer_token(request.headers.get("Authorization"))
    except AuthError as e:
        return jsonify({"error": str(e)}), 401

    try:
        tickers = get_all_tickers(user_id=auth.user_id)
        cached_data = get_price_movement_data(tickers)
        serialized_data = {}
        for ticker in tickers:
            payload = cached_data.get(ticker)
            if isinstance(payload, list):
                serialized_data[ticker] = {"candles": payload, "summary": {"previousClose": None}}
                continue
            if not isinstance(payload, dict) or not payload:
                serialized_data[ticker] = {"candles": [], "summary": {"previousClose": None}}
                continue

            candles = payload.get("candles") if isinstance(payload.get("candles"), list) else []
            summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
            prev_close = summary.get("previousClose")
            if prev_close is None:
                prev_close = summary.get("prevClose")

            serialized_data[ticker] = {
                "candles": candles,
                "summary": {"previousClose": prev_close},
            }

        return jsonify(serialized_data), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500



@tickers_blueprint.route('/api/tickers', methods=['POST'])
def add_ticker():
    """
    Expects JSON:
      { "ticker": "TSLA" }
    or 
      { "tickers": ["TSLA", "AAPL"] }
    """
    try:
        auth = authenticate_bearer_token(request.headers.get("Authorization"))
    except AuthError as e:
        return jsonify({"error": str(e)}), 401

    try:
        data = request.get_json() or {}
        # Instead of inserting globally, we add the ticker(s) to the user’s list.
        # Example: add_ticker_to_user_list(user_id, ticker)
        if "ticker" in data:
            add_ticker_to_user_list(auth.user_id, data["ticker"])
        elif "tickers" in data:
            for t in data["tickers"]:
                add_ticker_to_user_list(auth.user_id, t)
        else:
            return jsonify({'error': 'ticker or tickers field is required'}), 400

        return jsonify({'status': 'success'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@tickers_blueprint.route('/api/tickers', methods=['PUT'])
def replace_tickers():
    """
    Expects JSON:
      { "tickers": ["TSLA", "AAPL"] }
    Replaces the user's default watchlist with the provided tickers.
    """
    try:
        auth = authenticate_bearer_token(request.headers.get("Authorization"))
    except AuthError as e:
        return jsonify({"error": str(e)}), 401

    try:
        data = request.get_json() or {}
        tickers = data.get("tickers")
        if not isinstance(tickers, list):
            return jsonify({'error': 'tickers field is required'}), 400

        replace_user_watchlist(auth.user_id, tickers)
        return jsonify({'status': 'success'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@tickers_blueprint.route('/api/tickers', methods=['DELETE'])
def delete_ticker():
    """
    Expects JSON:
      { "ticker": "TSLA" }
    """
    try:
        auth = authenticate_bearer_token(request.headers.get("Authorization"))
    except AuthError as e:
        return jsonify({"error": str(e)}), 401

    try:
        data = request.get_json() or {}
        ticker = data.get("ticker")

        if not ticker:
            return jsonify({'error': 'ticker is required'}), 400

        # Remove ticker from the user’s watchlist only
        remove_ticker_from_user_list(auth.user_id, ticker)
        return jsonify({'status': 'deleted'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
