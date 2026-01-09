# routes/world_markets_routes.py

from flask import Blueprint, jsonify

from analysis.world_markets import fetch_world_market_moves
from utils.serialization import convert_to_python_types

world_markets_blueprint = Blueprint("world_markets", __name__)


@world_markets_blueprint.route("/api/world-markets", methods=["GET"])
def world_markets_snapshot():
    try:
        payload = fetch_world_market_moves()
        payload = convert_to_python_types(payload)
        return jsonify(payload), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
