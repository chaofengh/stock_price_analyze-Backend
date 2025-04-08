# routes/ticker_logo_routes.py

import base64
import requests
from flask import Blueprint, request, jsonify
from dotenv import load_dotenv
import os

from database.ticker_repository import (
    get_logo_base64_for_symbol,
    update_logo_base64_for_symbol,
)

load_dotenv()
FINNHUB_API_KEY = os.getenv("finnhub_api_key")

ticker_logo_blueprint = Blueprint("ticker_logo_blueprint", __name__)

@ticker_logo_blueprint.route("/api/tickers/<symbol>/logo", methods=["GET"])
def get_ticker_logo(symbol):
    """
    1) Check if 'logo_url_base64' is in the database.
       - If yes, return it.
       - If no, fetch from Finnhub, convert to Base64, store in DB, then return it.
    """
    # Check DB first
    current_logo_base64 = get_logo_base64_for_symbol(symbol)

    if current_logo_base64:
        return jsonify({"symbol": symbol, "logo_base64": current_logo_base64})
    
    # If it's None, fetch from Finnhub
    finnhub_url = f"https://finnhub.io/api/v1/stock/profile2?symbol={symbol}&token={FINNHUB_API_KEY}"
    try:
        resp = requests.get(finnhub_url)
        resp.raise_for_status()
        data = resp.json()
        logo_url = data.get("logo")

        # If no logo is available, just return a 404 or empty
        if not logo_url:
            return jsonify({"symbol": symbol, "logo_base64": None}), 404

        # Fetch the image
        img_resp = requests.get(logo_url)
        img_resp.raise_for_status()

        # Convert to Base64
        base64_str = base64.b64encode(img_resp.content).decode("utf-8")
        # Save to DB
        update_logo_base64_for_symbol(symbol, base64_str)

        return jsonify({"symbol": symbol, "logo_base64": base64_str})
    except Exception as e:
        print(f"Error fetching logo for {symbol} from Finnhub:", e)
        return jsonify({"symbol": symbol, "logo_base64": None}), 500
