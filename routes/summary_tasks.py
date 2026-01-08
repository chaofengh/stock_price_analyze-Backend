from analysis.summary import (
    get_summary,
    get_summary_peers,
    get_summary_fundamentals,
    get_summary_peer_averages,
)
from analysis.data_fetcher_fundamentals_extract import is_empty_fundamentals
from utils.serialization import convert_to_python_types
from .summary_cache import (
    SUMMARY_CACHE,
    PEERS_CACHE,
    FUNDAMENTALS_CACHE,
    PEER_AVG_CACHE,
)

_PEER_AVG_KEYS = (
    "avg_peer_trailingPE",
    "avg_peer_forwardPE",
    "avg_peer_PEG",
    "avg_peer_PGI",
    "avg_peer_beta",
)


def _payload_has_any(payload: dict, keys: tuple) -> bool:
    if not isinstance(payload, dict):
        return False
    return any(payload.get(key) is not None for key in keys)


def compute_summary_async(symbol: str):
    try:
        payload = convert_to_python_types(get_summary(symbol))
        if isinstance(payload, dict):
            payload["symbol"] = symbol
        SUMMARY_CACHE.set(symbol, payload)
    finally:
        SUMMARY_CACHE.finish_inflight(symbol)


def compute_peers_async(symbol: str):
    try:
        payload = convert_to_python_types(get_summary_peers(symbol))
        if isinstance(payload, dict):
            payload["symbol"] = symbol
        PEERS_CACHE.set(symbol, payload)
    finally:
        PEERS_CACHE.finish_inflight(symbol)


def compute_fundamentals_async(symbol: str):
    try:
        payload = convert_to_python_types(get_summary_fundamentals(symbol))
        if isinstance(payload, dict):
            payload["symbol"] = symbol
        if not is_empty_fundamentals(payload):
            FUNDAMENTALS_CACHE.set(symbol, payload)
    finally:
        FUNDAMENTALS_CACHE.finish_inflight(symbol)


def compute_peer_avg_async(symbol: str):
    try:
        payload = convert_to_python_types(get_summary_peer_averages(symbol))
        if isinstance(payload, dict):
            payload["symbol"] = symbol
        if _payload_has_any(payload, _PEER_AVG_KEYS):
            PEER_AVG_CACHE.set(symbol, payload)
    finally:
        PEER_AVG_CACHE.finish_inflight(symbol)
