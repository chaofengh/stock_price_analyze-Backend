# routes/financials_routes.py

from flask import Blueprint, jsonify
from analysis.data_fetcher import fetch_financials

financials_blueprint = Blueprint('financials', __name__)

@financials_blueprint.route('/api/financials/cash_flow/<symbol>', methods=['GET'])
def get_cash_flow(symbol):
    """
    Fetches and returns the cash flow statement for a given stock symbol.
    Example request: GET /api/financials/cash_flow/AAPL
    """
    try:
        data = fetch_financials(symbol, statements="cash_flow")
        return jsonify(data.get("cash_flow", {})), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@financials_blueprint.route('/api/financials/balance_sheet/<symbol>', methods=['GET'])
def get_balance_sheet(symbol):
    """
    Fetches and returns the balance sheet for a given stock symbol.
    Example request: GET /api/financials/balance_sheet/AAPL
    """
    try:
        data = fetch_financials(symbol, statements="balance_sheet")
        return jsonify(data.get("balance_sheet", {})), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
