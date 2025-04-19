from flask import Blueprint, request, jsonify
from backtest_strategies.runner import run_backtest_grid

backtest_blueprint = Blueprint("backtest_blueprint", __name__)

@backtest_blueprint.route("/api/backtest", methods=["GET"])
def backtest_endpoint():
    """
    Run a grid‑search across ORB & Reverse‑ORB and return
    the *top 10* scenarios by win‑rate for the requested ticker.
    Params:
        ticker – stock symbol (default: AAPL)
        days   – look‑back window in trading days (default: 30)
    """
    ticker = request.args.get("ticker", "AAPL").upper()
    days   = int(request.args.get("days", 30))

    payload = run_backtest_grid(ticker=ticker, days=days)
    return jsonify(payload), 200
