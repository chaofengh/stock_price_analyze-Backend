"""
summary_cache.py
Purpose: cache fundamentals and peer lists for summary endpoints.
"""
from utils.ttl_cache import TTLCache
from .fundamentals import get_fundamentals, get_fundamentals_light, get_peers
from .data_fetcher_fundamentals_extract import is_empty_fundamentals

_FUNDAMENTALS_CACHE = TTLCache(ttl_seconds=60 * 60 * 12, max_size=512)
_PEERS_CACHE = TTLCache(ttl_seconds=60 * 60 * 12, max_size=512)
_PEERS_EMPTY_CACHE = TTLCache(ttl_seconds=60 * 5, max_size=512)
_PEER_FUNDAMENTALS_CACHE = TTLCache(ttl_seconds=60 * 60 * 12, max_size=2048)
_PEER_FUNDAMENTALS_EMPTY_CACHE = TTLCache(ttl_seconds=60 * 5, max_size=2048)
_NO_DATA = object()


def normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper() if symbol else ""


def _is_empty_fundamentals(payload: dict) -> bool:
    return is_empty_fundamentals(payload)


def _cached_lookup(key, cache, empty_cache, fetch_fn, is_empty):
    cached = cache.get(key, _NO_DATA)
    if cached is not _NO_DATA:
        return cached
    cached_empty = empty_cache.get(key, _NO_DATA)
    if cached_empty is not _NO_DATA:
        return cached_empty
    value = fetch_fn()
    if is_empty(value):
        empty_cache.set(key, value)
    else:
        cache.set(key, value)
    return value


def get_cached_fundamentals(symbol: str) -> dict:
    sym = normalize_symbol(symbol)
    cached = _FUNDAMENTALS_CACHE.get(sym, _NO_DATA)
    if cached is not _NO_DATA:
        return cached
    value = get_fundamentals(sym)
    if not _is_empty_fundamentals(value):
        _FUNDAMENTALS_CACHE.set(sym, value)
    return value


def get_cached_peers(symbol: str) -> list:
    sym = normalize_symbol(symbol)
    return _cached_lookup(
        sym,
        _PEERS_CACHE,
        _PEERS_EMPTY_CACHE,
        lambda: get_peers(sym),
        lambda value: not value,
    )


def get_cached_peer_fundamentals(symbol: str) -> dict:
    sym = normalize_symbol(symbol)
    return _cached_lookup(
        sym,
        _PEER_FUNDAMENTALS_CACHE,
        _PEER_FUNDAMENTALS_EMPTY_CACHE,
        lambda: get_fundamentals_light(sym),
        _is_empty_fundamentals,
    )
