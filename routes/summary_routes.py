from flask import Blueprint, jsonify, request

from analysis.summary import (
    get_summary,
    get_summary_bundle,
    get_summary_overview,
    get_summary_peers,
    get_summary_fundamentals,
    get_summary_peer_averages,
    get_entry_decision,
)
from utils.serialization import convert_to_python_types

summary_blueprint = Blueprint("summary", __name__)


def _get_symbol() -> str:
    symbol = request.args.get("symbol", default="QQQ")
    return symbol.strip().upper()


@summary_blueprint.route("/api/summary", methods=["GET"])
def summary_endpoint():
    """
    Example usage:
      GET /api/summary?symbol=QQQ
    Returns the analysis summary as JSON.
    """
    symbol = _get_symbol()
    try:
        payload = convert_to_python_types(get_summary(symbol))
        return jsonify(payload), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@summary_blueprint.route("/api/summary/overview", methods=["GET"])
def summary_overview_endpoint():
    """
    Lightweight overview: fundamentals, peer averages, and peer info.
    Example usage:
      GET /api/summary/overview?symbol=QQQ
    """
    symbol = _get_symbol()
    try:
        overview = convert_to_python_types(get_summary_overview(symbol))
        return jsonify(overview), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@summary_blueprint.route("/api/summary/peers", methods=["GET"])
def summary_peers_endpoint():
    """
    Lightweight peer info for People Also View.
    Example usage:
      GET /api/summary/peers?symbol=QQQ
    """
    symbol = _get_symbol()
    try:
        payload = convert_to_python_types(get_summary_peers(symbol))
        return jsonify(payload), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@summary_blueprint.route("/api/summary/fundamentals", methods=["GET"])
def summary_fundamentals_endpoint():
    """
    Lightweight fundamentals for a single ticker.
    Example usage:
      GET /api/summary/fundamentals?symbol=QQQ
    """
    symbol = _get_symbol()
    try:
        payload = convert_to_python_types(get_summary_fundamentals(symbol))
        return jsonify(payload), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@summary_blueprint.route("/api/summary/peer-averages", methods=["GET"])
def summary_peer_averages_endpoint():
    """
    Peer averages for valuation metrics.
    Example usage:
      GET /api/summary/peer-averages?symbol=QQQ
    """
    symbol = _get_symbol()
    try:
        payload = convert_to_python_types(get_summary_peer_averages(symbol))
        return jsonify(payload), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@summary_blueprint.route("/api/summary/bundle", methods=["GET"])
def summary_bundle_endpoint():
    """
    Full summary payload (chart + fundamentals + peers + peer averages).
    Example usage:
      GET /api/summary/bundle?symbol=QQQ
    """
    symbol = _get_symbol()
    try:
        payload = convert_to_python_types(get_summary_bundle(symbol))
        return jsonify(payload), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@summary_blueprint.route("/api/summary/entry-decision", methods=["GET"])
def summary_entry_decision_endpoint():
    """
    Two-stage Bollinger decision object + 1Y prediction-accuracy backtest.
    Example usage:
      GET /api/summary/entry-decision?symbol=QQQ&as_of_date=2026-04-15
    """
    symbol = _get_symbol()
    as_of_date = request.args.get("as_of_date", default=None, type=str)
    try:
        payload = convert_to_python_types(get_entry_decision(symbol, as_of_date=as_of_date))
        return jsonify(payload), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500
