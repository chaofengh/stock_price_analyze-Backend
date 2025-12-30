import threading
from flask import Blueprint, current_app, jsonify, request
from analysis.summary import (
    get_summary,
    get_summary_overview,
    get_summary_peers,
    get_summary_fundamentals,
    get_summary_peer_averages,
)
from utils.serialization import convert_to_python_types
from .summary_cache import (
    SUMMARY_CACHE,
    OVERVIEW_CACHE,
    PEERS_CACHE,
    FUNDAMENTALS_CACHE,
    PEER_AVG_CACHE,
)
from .summary_tasks import (
    compute_summary_async,
    compute_peers_async,
)

summary_blueprint = Blueprint('summary', __name__)


def _get_symbol() -> str:
    symbol = request.args.get('symbol', default='QQQ')
    return symbol.strip().upper()


def _use_cache() -> bool:
    return not current_app.config.get("TESTING", False)


def _read_cache(cache, symbol: str):
    cached = cache.get(symbol)
    if not isinstance(cached, dict):
        return cached
    cached_symbol = cached.get("symbol")
    if cached_symbol and cached_symbol != symbol:
        current_app.logger.warning(
            "Summary cache symbol mismatch: %s != %s", cached_symbol, symbol
        )
        return None
    return cached


def _pending_response(symbol: str, status_code: int = 200):
    return jsonify({
        'symbol': symbol,
        'status': 'pending',
        'retry_after_seconds': 1,
    }), status_code


def _is_empty_fundamentals(payload) -> bool:
    if not isinstance(payload, dict):
        return True
    for key in (
        "trailingPE",
        "forwardPE",
        "PEG",
        "PGI",
        "dividendYield",
        "beta",
        "marketCap",
    ):
        if payload.get(key) is not None:
            return False
    return True

@summary_blueprint.route('/api/summary', methods=['GET'])
def summary_endpoint():
    """
    Example usage:
      GET /api/summary?symbol=QQQ
    Returns the analysis summary as JSON.
    """
    symbol = _get_symbol()
    try:
        if _use_cache():
            cached = _read_cache(SUMMARY_CACHE, symbol)
            if cached is not None:
                return jsonify(cached), 200
            if SUMMARY_CACHE.start_inflight(symbol):
                threading.Thread(
                    target=compute_summary_async,
                    args=(symbol,),
                    daemon=True,
                ).start()
            return _pending_response(symbol, status_code=200)
        payload = convert_to_python_types(get_summary(symbol))
        return jsonify(payload), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@summary_blueprint.route('/api/summary/overview', methods=['GET'])
def summary_overview_endpoint():
    """
    Lightweight overview: fundamentals, peer averages, and peer info.
    Example usage:
      GET /api/summary/overview?symbol=QQQ
    """
    symbol = _get_symbol()
    try:
        if _use_cache():
            cached = _read_cache(OVERVIEW_CACHE, symbol)
            if cached is not None:
                return jsonify(cached), 200
        overview = get_summary_overview(symbol)
        overview = convert_to_python_types(overview)
        if _use_cache():
            OVERVIEW_CACHE.set(symbol, overview)
        return jsonify(overview), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@summary_blueprint.route('/api/summary/peers', methods=['GET'])
def summary_peers_endpoint():
    """
    Lightweight peer info for People Also View.
    Example usage:
      GET /api/summary/peers?symbol=QQQ
    """
    symbol = _get_symbol()
    try:
        if _use_cache():
            cached = _read_cache(PEERS_CACHE, symbol)
            if cached is not None:
                return jsonify(cached), 200
            if PEERS_CACHE.start_inflight(symbol):
                threading.Thread(
                    target=compute_peers_async,
                    args=(symbol,),
                    daemon=True,
                ).start()
            return _pending_response(symbol, status_code=200)
        payload = convert_to_python_types(get_summary_peers(symbol))
        return jsonify(payload), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@summary_blueprint.route('/api/summary/fundamentals', methods=['GET'])
def summary_fundamentals_endpoint():
    """
    Lightweight fundamentals for a single ticker.
    Example usage:
      GET /api/summary/fundamentals?symbol=QQQ
    """
    symbol = _get_symbol()
    try:
        if _use_cache():
            cached = _read_cache(FUNDAMENTALS_CACHE, symbol)
            if cached is not None and not _is_empty_fundamentals(cached):
                return jsonify(cached), 200
            if cached is not None:
                FUNDAMENTALS_CACHE.delete(symbol)
            payload = convert_to_python_types(get_summary_fundamentals(symbol))
            if not _is_empty_fundamentals(payload):
                FUNDAMENTALS_CACHE.set(symbol, payload)
            return jsonify(payload), 200
        payload = convert_to_python_types(get_summary_fundamentals(symbol))
        return jsonify(payload), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@summary_blueprint.route('/api/summary/peer-averages', methods=['GET'])
def summary_peer_averages_endpoint():
    """
    Peer averages for valuation metrics.
    Example usage:
      GET /api/summary/peer-averages?symbol=QQQ
    """
    symbol = _get_symbol()
    try:
        if _use_cache():
            cached = _read_cache(PEER_AVG_CACHE, symbol)
            if cached is not None:
                return jsonify(cached), 200
            payload = convert_to_python_types(get_summary_peer_averages(symbol))
            PEER_AVG_CACHE.set(symbol, payload)
            return jsonify(payload), 200
        payload = convert_to_python_types(get_summary_peer_averages(symbol))
        return jsonify(payload), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
