import threading
from flask import Blueprint, current_app, jsonify, request
from analysis.summary import (
    get_summary,
    get_summary_overview,
    get_summary_peers,
    get_summary_fundamentals,
    get_summary_peer_averages,
)
from analysis.data_fetcher_fundamentals_extract import is_empty_fundamentals
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


_PEER_AVG_KEYS = (
    "avg_peer_trailingPE",
    "avg_peer_forwardPE",
    "avg_peer_PEG",
    "avg_peer_PGI",
    "avg_peer_beta",
)
_VALUATION_KEYS = (
    "trailingPE",
    "forwardPE",
    "PEG",
    "PGI",
    "dividendYield",
    "beta",
    "marketCap",
)
_OVERVIEW_KEYS = (
    "trailingPE",
    "forwardPE",
    "PEG",
    "PGI",
    "dividendYield",
    "beta",
    "marketCap",
) + _PEER_AVG_KEYS


def _payload_has_any(payload: dict, keys: tuple) -> bool:
    if not isinstance(payload, dict):
        return False
    return any(payload.get(key) is not None for key in keys)


def _has_valuation_metrics(payload: dict) -> bool:
    return _payload_has_any(payload, _VALUATION_KEYS)


def _has_peer_info(payload: dict) -> bool:
    if not isinstance(payload, dict):
        return False
    peer_info = payload.get("peer_info")
    return isinstance(peer_info, dict)

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
            if cached is not None and _payload_has_any(cached, _OVERVIEW_KEYS):
                return jsonify(cached), 200
            if cached is not None:
                OVERVIEW_CACHE.delete(symbol)
        overview = get_summary_overview(symbol)
        overview = convert_to_python_types(overview)
        if _use_cache():
            if _payload_has_any(overview, _OVERVIEW_KEYS):
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
            if cached is not None and _has_peer_info(cached):
                return jsonify(cached), 200
            if cached is not None:
                PEERS_CACHE.delete(symbol)
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
            if cached is not None and not is_empty_fundamentals(cached):
                return jsonify(cached), 200
            if cached is not None:
                FUNDAMENTALS_CACHE.delete(symbol)
            payload = convert_to_python_types(get_summary_fundamentals(symbol))
            if is_empty_fundamentals(payload):
                return _pending_response(symbol, status_code=200)
            FUNDAMENTALS_CACHE.set(symbol, payload)
            return jsonify(payload), 200
        payload = convert_to_python_types(get_summary_fundamentals(symbol))
        if is_empty_fundamentals(payload):
            return _pending_response(symbol, status_code=200)
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
        fundamentals = None
        if _use_cache():
            fundamentals = _read_cache(FUNDAMENTALS_CACHE, symbol)
        if fundamentals is None or not _has_valuation_metrics(fundamentals):
            fundamentals = convert_to_python_types(get_summary_fundamentals(symbol))
            if not _has_valuation_metrics(fundamentals):
                return _pending_response(symbol, status_code=200)
            if _use_cache():
                FUNDAMENTALS_CACHE.set(symbol, fundamentals)

        if _use_cache():
            cached = _read_cache(PEER_AVG_CACHE, symbol)
            if cached is not None and _payload_has_any(cached, _PEER_AVG_KEYS):
                return jsonify(cached), 200
            if cached is not None:
                PEER_AVG_CACHE.delete(symbol)
            payload = convert_to_python_types(get_summary_peer_averages(symbol))
            if _payload_has_any(payload, _PEER_AVG_KEYS):
                PEER_AVG_CACHE.set(symbol, payload)
            return jsonify(payload), 200
        payload = convert_to_python_types(get_summary_peer_averages(symbol))
        return jsonify(payload), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
