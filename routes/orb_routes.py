# routes/orb_routes.py

from flask import Blueprint, request, jsonify
from analysis.opening_range_breakout import run_opening_range_breakout_tests

orb_blueprint = Blueprint("orb_blueprint", __name__)

@orb_blueprint.route("/api/opening_range_breakout", methods=["GET"])
def opening_range_breakout_endpoint():
    """
    Endpoint to run multiple Opening Range Breakout scenarios
    for a given ticker and return the performance metrics.
    """
    ticker = request.args.get("ticker", "AAPL")
    days = int(request.args.get("days", 30))

    # This function will run all the different ORB scenarios
    # and return a list of dictionaries with metrics
    results = run_opening_range_breakout_tests(ticker, days)

    return jsonify(results)
