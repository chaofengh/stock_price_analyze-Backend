# routes/world_markets_routes.py

import threading

from flask import Blueprint, jsonify

from analysis.world_markets import WORLD_MARKETS, fetch_world_market_moves
from utils.serialization import convert_to_python_types
from utils.ttl_cache import TTLCache

world_markets_blueprint = Blueprint("world_markets", __name__)
_WORLD_MARKETS_CACHE = TTLCache(ttl_seconds=300, max_size=2)
_WORLD_MARKETS_STALE_CACHE = TTLCache(ttl_seconds=60 * 60 * 12, max_size=2)
_WORLD_MARKETS_LOCK = threading.Lock()
_WORLD_MARKETS_INFLIGHT = False


def _build_pending_snapshot() -> dict:
    return {
        "status": "pending",
        "retry_after_seconds": 2,
        "as_of": None,
        "source": "yfinance",
        "markets": [
            {
                "id": market["id"],
                "label": market["label"],
                "ticker": market["ticker"],
                "name": market["name"],
                "percent_change": None,
                "last_close": None,
                "previous_close": None,
                "last_close_at": None,
                "used_fallback": False,
            }
            for market in WORLD_MARKETS
        ],
    }


def _refresh_world_markets_async():
    def _run():
        global _WORLD_MARKETS_INFLIGHT
        try:
            payload = fetch_world_market_moves()
            payload = convert_to_python_types(payload)
            _WORLD_MARKETS_CACHE.set("snapshot", payload)
            _WORLD_MARKETS_STALE_CACHE.set("snapshot", payload)
        except Exception:
            pass
        finally:
            with _WORLD_MARKETS_LOCK:
                _WORLD_MARKETS_INFLIGHT = False

    global _WORLD_MARKETS_INFLIGHT
    with _WORLD_MARKETS_LOCK:
        if _WORLD_MARKETS_INFLIGHT:
            return
        _WORLD_MARKETS_INFLIGHT = True

    threading.Thread(target=_run, daemon=True).start()


@world_markets_blueprint.route("/api/world-markets", methods=["GET"])
def world_markets_snapshot():
    try:
        cached = _WORLD_MARKETS_CACHE.get("snapshot", None)
        if cached is not None:
            return jsonify(cached), 200

        stale = _WORLD_MARKETS_STALE_CACHE.get("snapshot", None)
        _refresh_world_markets_async()
        if stale is not None:
            return jsonify(stale), 200

        return jsonify(_build_pending_snapshot()), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
