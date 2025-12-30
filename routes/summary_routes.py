import threading
import time
from flask import Blueprint, current_app, jsonify, request
from analysis.summary import (
    get_summary,
    get_summary_overview,
    get_summary_peers,
    get_summary_fundamentals,
    get_summary_peer_averages,
)

# Utility for converting to native Python objects
from utils.serialization import convert_to_python_types

summary_blueprint = Blueprint('summary', __name__)
_SUMMARY_CACHE = {}
_SUMMARY_CACHE_LOCK = threading.Lock()
_SUMMARY_INFLIGHT = set()
_SUMMARY_TTL_SECONDS = 60
_OVERVIEW_CACHE = {}
_OVERVIEW_CACHE_LOCK = threading.Lock()
_OVERVIEW_TTL_SECONDS = 600
_PEERS_CACHE = {}
_PEERS_CACHE_LOCK = threading.Lock()
_PEERS_INFLIGHT = set()
_PEERS_TTL_SECONDS = 300
_FUNDAMENTALS_CACHE = {}
_FUNDAMENTALS_CACHE_LOCK = threading.Lock()
_FUNDAMENTALS_INFLIGHT = set()
_FUNDAMENTALS_TTL_SECONDS = 600
_PEER_AVG_CACHE = {}
_PEER_AVG_CACHE_LOCK = threading.Lock()
_PEER_AVG_INFLIGHT = set()
_PEER_AVG_TTL_SECONDS = 600
_FUNDAMENTALS_KEYS = (
    "trailingPE",
    "forwardPE",
    "PEG",
    "PGI",
    "dividendYield",
    "beta",
    "marketCap",
)


def _get_cached_summary(symbol: str):
    now = time.time()
    with _SUMMARY_CACHE_LOCK:
        cached = _SUMMARY_CACHE.get(symbol)
        if not cached:
            return None
        payload, expires_at = cached
        if expires_at <= now:
            _SUMMARY_CACHE.pop(symbol, None)
            return None
        return payload


def _set_cached_summary(symbol: str, payload: dict):
    with _SUMMARY_CACHE_LOCK:
        _SUMMARY_CACHE[symbol] = (payload, time.time() + _SUMMARY_TTL_SECONDS)


def _compute_summary_async(symbol: str):
    try:
        df_summary = get_summary(symbol)
        df_summary = convert_to_python_types(df_summary)
        _set_cached_summary(symbol, df_summary)
    except Exception:
        pass
    finally:
        with _SUMMARY_CACHE_LOCK:
            _SUMMARY_INFLIGHT.discard(symbol)


def _get_cached_overview(symbol: str):
    now = time.time()
    with _OVERVIEW_CACHE_LOCK:
        cached = _OVERVIEW_CACHE.get(symbol)
        if not cached:
            return None
        payload, expires_at = cached
        if expires_at <= now:
            _OVERVIEW_CACHE.pop(symbol, None)
            return None
        return payload


def _set_cached_overview(symbol: str, payload: dict):
    with _OVERVIEW_CACHE_LOCK:
        _OVERVIEW_CACHE[symbol] = (payload, time.time() + _OVERVIEW_TTL_SECONDS)


def _get_cached_peers(symbol: str):
    now = time.time()
    with _PEERS_CACHE_LOCK:
        cached = _PEERS_CACHE.get(symbol)
        if not cached:
            return None
        payload, expires_at = cached
        if expires_at <= now:
            _PEERS_CACHE.pop(symbol, None)
            return None
        return payload


def _set_cached_peers(symbol: str, payload: dict):
    with _PEERS_CACHE_LOCK:
        _PEERS_CACHE[symbol] = (payload, time.time() + _PEERS_TTL_SECONDS)


def _compute_peers_async(symbol: str):
    try:
        peers_payload = get_summary_peers(symbol)
        peers_payload = convert_to_python_types(peers_payload)
        _set_cached_peers(symbol, peers_payload)
    except Exception:
        pass
    finally:
        with _PEERS_CACHE_LOCK:
            _PEERS_INFLIGHT.discard(symbol)


def _get_cached_fundamentals_payload(symbol: str):
    now = time.time()
    with _FUNDAMENTALS_CACHE_LOCK:
        cached = _FUNDAMENTALS_CACHE.get(symbol)
        if not cached:
            return None
        payload, expires_at = cached
        if expires_at <= now:
            _FUNDAMENTALS_CACHE.pop(symbol, None)
            return None
        if not any(payload.get(key) is not None for key in _FUNDAMENTALS_KEYS):
            _FUNDAMENTALS_CACHE.pop(symbol, None)
            return None
        return payload


def _set_cached_fundamentals_payload(symbol: str, payload: dict):
    with _FUNDAMENTALS_CACHE_LOCK:
        if not any(payload.get(key) is not None for key in _FUNDAMENTALS_KEYS):
            return
        _FUNDAMENTALS_CACHE[symbol] = (payload, time.time() + _FUNDAMENTALS_TTL_SECONDS)


def _compute_fundamentals_async(symbol: str):
    try:
        payload = get_summary_fundamentals(symbol)
        payload = convert_to_python_types(payload)
        _set_cached_fundamentals_payload(symbol, payload)
    except Exception:
        pass
    finally:
        with _FUNDAMENTALS_CACHE_LOCK:
            _FUNDAMENTALS_INFLIGHT.discard(symbol)


def _get_cached_peer_avg_payload(symbol: str):
    now = time.time()
    with _PEER_AVG_CACHE_LOCK:
        cached = _PEER_AVG_CACHE.get(symbol)
        if not cached:
            return None
        payload, expires_at = cached
        if expires_at <= now:
            _PEER_AVG_CACHE.pop(symbol, None)
            return None
        return payload


def _set_cached_peer_avg_payload(symbol: str, payload: dict):
    with _PEER_AVG_CACHE_LOCK:
        _PEER_AVG_CACHE[symbol] = (payload, time.time() + _PEER_AVG_TTL_SECONDS)


def _compute_peer_avg_async(symbol: str):
    try:
        payload = get_summary_peer_averages(symbol)
        payload = convert_to_python_types(payload)
        _set_cached_peer_avg_payload(symbol, payload)
    except Exception:
        pass
    finally:
        with _PEER_AVG_CACHE_LOCK:
            _PEER_AVG_INFLIGHT.discard(symbol)

@summary_blueprint.route('/api/summary', methods=['GET'])
def summary_endpoint():
    """
    Example usage:
      GET /api/summary?symbol=QQQ
    Returns the analysis summary as JSON.
    """
    symbol = request.args.get('symbol', default='QQQ')
    symbol = symbol.strip().upper()
    try:
        use_cache = not current_app.config.get("TESTING", False)
        if use_cache:
            cached = _get_cached_summary(symbol)
            if cached is not None:
                return jsonify(cached), 200
            with _SUMMARY_CACHE_LOCK:
                if symbol not in _SUMMARY_INFLIGHT:
                    _SUMMARY_INFLIGHT.add(symbol)
                    threading.Thread(
                        target=_compute_summary_async,
                        args=(symbol,),
                        daemon=True,
                    ).start()
            return jsonify({
                'symbol': symbol,
                'status': 'pending',
                'retry_after_seconds': 1,
            }), 200
        df_summary = get_summary(symbol)
        df_summary = convert_to_python_types(df_summary)
        if use_cache:
            _set_cached_summary(symbol, df_summary)
        return jsonify(df_summary), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@summary_blueprint.route('/api/summary/overview', methods=['GET'])
def summary_overview_endpoint():
    """
    Lightweight overview: fundamentals, peer averages, and peer info.
    Example usage:
      GET /api/summary/overview?symbol=QQQ
    """
    symbol = request.args.get('symbol', default='QQQ')
    symbol = symbol.strip().upper()
    try:
        use_cache = not current_app.config.get("TESTING", False)
        if use_cache:
            cached = _get_cached_overview(symbol)
            if cached is not None:
                return jsonify(cached), 200
        overview = get_summary_overview(symbol)
        overview = convert_to_python_types(overview)
        if use_cache:
            _set_cached_overview(symbol, overview)
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
    symbol = request.args.get('symbol', default='QQQ')
    symbol = symbol.strip().upper()
    try:
        use_cache = not current_app.config.get("TESTING", False)
        if use_cache:
            cached = _get_cached_peers(symbol)
            if cached is not None:
                return jsonify(cached), 200
            with _PEERS_CACHE_LOCK:
                if symbol not in _PEERS_INFLIGHT:
                    _PEERS_INFLIGHT.add(symbol)
                    threading.Thread(
                        target=_compute_peers_async,
                        args=(symbol,),
                        daemon=True,
                    ).start()
            return jsonify({
                'symbol': symbol,
                'status': 'pending',
                'retry_after_seconds': 1,
            }), 200
        peers_payload = get_summary_peers(symbol)
        peers_payload = convert_to_python_types(peers_payload)
        return jsonify(peers_payload), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@summary_blueprint.route('/api/summary/fundamentals', methods=['GET'])
def summary_fundamentals_endpoint():
    """
    Lightweight fundamentals for a single ticker.
    Example usage:
      GET /api/summary/fundamentals?symbol=QQQ
    """
    symbol = request.args.get('symbol', default='QQQ')
    symbol = symbol.strip().upper()
    try:
        use_cache = not current_app.config.get("TESTING", False)
        if use_cache:
            cached = _get_cached_fundamentals_payload(symbol)
            if cached is not None:
                return jsonify(cached), 200
            with _FUNDAMENTALS_CACHE_LOCK:
                if symbol not in _FUNDAMENTALS_INFLIGHT:
                    _FUNDAMENTALS_INFLIGHT.add(symbol)
                    threading.Thread(
                        target=_compute_fundamentals_async,
                        args=(symbol,),
                        daemon=True,
                    ).start()
            return jsonify({
                'symbol': symbol,
                'status': 'pending',
                'retry_after_seconds': 1,
            }), 202
        payload = get_summary_fundamentals(symbol)
        payload = convert_to_python_types(payload)
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
    symbol = request.args.get('symbol', default='QQQ')
    symbol = symbol.strip().upper()
    try:
        use_cache = not current_app.config.get("TESTING", False)
        if use_cache:
            cached = _get_cached_peer_avg_payload(symbol)
            if cached is not None:
                return jsonify(cached), 200
            with _PEER_AVG_CACHE_LOCK:
                if symbol not in _PEER_AVG_INFLIGHT:
                    _PEER_AVG_INFLIGHT.add(symbol)
                    threading.Thread(
                        target=_compute_peer_avg_async,
                        args=(symbol,),
                        daemon=True,
                    ).start()
            return jsonify({
                'symbol': symbol,
                'status': 'pending',
                'retry_after_seconds': 1,
            }), 202
        payload = get_summary_peer_averages(symbol)
        payload = convert_to_python_types(payload)
        return jsonify(payload), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
