# routes/backtest_routes.py

from flask import Blueprint, request, jsonify
from backtest_strategies.runner import run_backtest

backtest_blueprint = Blueprint("backtest_blueprint", __name__)

@backtest_blueprint.route("/api/backtest", methods=["GET"])
def backtest_endpoint():
    """
    Endpoint to run multiple backtest scenarios for a given strategy,
    ticker, and number of days, then return performance metrics.
    """
    ticker = request.args.get("ticker", "AAPL")
    days = int(request.args.get("days", 30))
    strategy = request.args.get("strategy", "opening_range_breakout")

    # The run_backtest function calls the appropriate strategy
    results = run_backtest(ticker, days=days, interval="5m", strategy=strategy)
    return jsonify(results)
