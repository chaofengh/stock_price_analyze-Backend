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


def compute_summary_async(symbol: str):
    try:
        payload = convert_to_python_types(get_summary(symbol))
        SUMMARY_CACHE.set(symbol, payload)
    finally:
        SUMMARY_CACHE.finish_inflight(symbol)


def compute_peers_async(symbol: str):
    try:
        payload = convert_to_python_types(get_summary_peers(symbol))
        PEERS_CACHE.set(symbol, payload)
    finally:
        PEERS_CACHE.finish_inflight(symbol)


def compute_fundamentals_async(symbol: str):
    try:
        payload = convert_to_python_types(get_summary_fundamentals(symbol))
        FUNDAMENTALS_CACHE.set(symbol, payload)
    finally:
        FUNDAMENTALS_CACHE.finish_inflight(symbol)


def compute_peer_avg_async(symbol: str):
    try:
        payload = convert_to_python_types(get_summary_peer_averages(symbol))
        PEER_AVG_CACHE.set(symbol, payload)
    finally:
        PEER_AVG_CACHE.finish_inflight(symbol)
