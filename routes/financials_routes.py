# routes/financials_routes.py

from flask import Blueprint, jsonify
from analysis.data_fetcher import fetch_financials

financials_blueprint = Blueprint('financials', __name__)

STATEMENT_TYPES = {"cash_flow", "balance_sheet", "income_statement"}


def _fetch_statement(symbol, statement_type):
    try:
        data = fetch_financials(symbol, statements=statement_type)
        return jsonify(data.get(statement_type, {})), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@financials_blueprint.route('/api/financials/<statement_type>/<symbol>', methods=['GET'])
def get_financial_statement(statement_type, symbol):
    """
    Fetches and returns a financial statement for a given stock symbol.
    Example request: GET /api/financials/income_statement/AAPL
    """
    if statement_type not in STATEMENT_TYPES:
        return jsonify({"error": "Invalid statement type"}), 400
    return _fetch_statement(symbol, statement_type)


@financials_blueprint.route('/api/financials/cash_flow/<symbol>', methods=['GET'])
def get_cash_flow(symbol):
    """
    Fetches and returns the cash flow statement for a given stock symbol.
    Example request: GET /api/financials/cash_flow/AAPL
    """
    return _fetch_statement(symbol, "cash_flow")


@financials_blueprint.route('/api/financials/balance_sheet/<symbol>', methods=['GET'])
def get_balance_sheet(symbol):
    """
    Fetches and returns the balance sheet for a given stock symbol.
    Example request: GET /api/financials/balance_sheet/AAPL
    """
    return _fetch_statement(symbol, "balance_sheet")


@financials_blueprint.route('/api/financials/income_statement/<symbol>', methods=['GET'])
def get_income_statement(symbol):
    """
    Fetches and returns the income statement for a given stock symbol.
    Example request: GET /api/financials/income_statement/AAPL
    """
    return _fetch_statement(symbol, "income_statement")
