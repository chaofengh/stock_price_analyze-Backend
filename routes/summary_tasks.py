from analysis.summary import (
    get_summary,
    get_summary_peers,
    get_summary_fundamentals,
    get_summary_peer_averages,
)
from utils.serialization import convert_to_python_types
from .summary_cache import (
    SUMMARY_CACHE,
    PEERS_CACHE,
    FUNDAMENTALS_CACHE,
    PEER_AVG_CACHE,
)


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
        if not _is_empty_fundamentals(payload):
            FUNDAMENTALS_CACHE.set(symbol, payload)
    finally:
        FUNDAMENTALS_CACHE.finish_inflight(symbol)


def compute_peer_avg_async(symbol: str):
    try:
        payload = convert_to_python_types(get_summary_peer_averages(symbol))
        if isinstance(payload, dict):
            payload["symbol"] = symbol
        PEER_AVG_CACHE.set(symbol, payload)
    finally:
        PEER_AVG_CACHE.finish_inflight(symbol)
