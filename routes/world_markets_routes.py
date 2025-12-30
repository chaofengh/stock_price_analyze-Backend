# routes/world_markets_routes.py

from flask import Blueprint, jsonify

from analysis.world_markets import fetch_world_market_moves
from utils.serialization import convert_to_python_types
from utils.ttl_cache import TTLCache

world_markets_blueprint = Blueprint("world_markets", __name__)
_WORLD_MARKETS_CACHE = TTLCache(ttl_seconds=300, max_size=2)


@world_markets_blueprint.route("/api/world-markets", methods=["GET"])
def world_markets_snapshot():
    try:
        cached = _WORLD_MARKETS_CACHE.get("snapshot", None)
        if cached is None:
            payload = fetch_world_market_moves()
            payload = convert_to_python_types(payload)
            _WORLD_MARKETS_CACHE.set("snapshot", payload)
            return jsonify(payload), 200
        return jsonify(cached), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
