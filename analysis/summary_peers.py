"""
summary_peers.py
Purpose: build fundamentals and peer comparison payloads for summary endpoints.
"""
import copy
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
from .data_fetcher_utils import normalize_symbol
from .fundamentals import get_fundamentals, get_peers
from .summary_core import get_summary
from .summary_peer_utils import (
    PEER_METRICS,
    normalize_peers,
    get_peer_info,
    get_peer_metric_averages,
)

_MAX_PEERS = 6
_PEER_AVG_MAX_PEERS = 12

_CACHE_LOCK = threading.Lock()
_SUMMARY_CACHE: dict[tuple, tuple[float, object]] = {}


def _ttl_seconds(env_key: str, default: int) -> int:
    raw = os.getenv(env_key)
    if raw is None or raw == "":
        return default
    try:
        parsed = int(raw)
    except (TypeError, ValueError):
        return default
    return max(0, parsed)


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


_SUMMARY_TTL_SECONDS = _ttl_seconds("SUMMARY_CACHE_TTL_SECONDS", 20)
_PEERS_TTL_SECONDS = _ttl_seconds("SUMMARY_PEERS_CACHE_TTL_SECONDS", 120)
_FUNDAMENTALS_TTL_SECONDS = _ttl_seconds("SUMMARY_FUNDAMENTALS_CACHE_TTL_SECONDS", 300)
_PEER_INFO_TTL_SECONDS = _ttl_seconds("SUMMARY_PEER_INFO_CACHE_TTL_SECONDS", 60)
_PEER_AVG_TTL_SECONDS = _ttl_seconds("SUMMARY_PEER_AVG_CACHE_TTL_SECONDS", 180)
_SUMMARY_INCLUDE_ALPHA_FUNDAMENTALS = _bool_env(
    "SUMMARY_INCLUDE_ALPHA_FUNDAMENTALS",
    False,
)


def _cache_get_or_build(key: tuple, ttl_seconds: int, builder):
    if ttl_seconds <= 0:
        return builder()

    now = time.time()
    with _CACHE_LOCK:
        hit = _SUMMARY_CACHE.get(key)
        if hit is not None:
            expires_at, cached_value = hit
            if expires_at > now:
                return copy.deepcopy(cached_value)

    value = builder()
    expires_at = now + ttl_seconds
    with _CACHE_LOCK:
        _SUMMARY_CACHE[key] = (expires_at, copy.deepcopy(value))
    return value


def _peers_key(peers: list) -> tuple:
    return tuple(peers or [])


def _get_summary_cached(symbol: str) -> dict:
    return _cache_get_or_build(
        ("summary", symbol),
        _SUMMARY_TTL_SECONDS,
        lambda: get_summary(symbol),
    )


def _get_peers_cached(symbol: str) -> list:
    return _cache_get_or_build(
        ("peers", symbol),
        _PEERS_TTL_SECONDS,
        lambda: get_peers(symbol),
    )


def _get_fundamentals_cached(symbol: str) -> dict:
    return _cache_get_or_build(
        ("fundamentals", symbol),
        _FUNDAMENTALS_TTL_SECONDS,
        lambda: get_fundamentals(
            symbol,
            include_alpha=_SUMMARY_INCLUDE_ALPHA_FUNDAMENTALS,
        ),
    )


def _get_peer_info_cached(peers: list) -> dict:
    key = ("peer_info",) + _peers_key(peers)
    return _cache_get_or_build(
        key,
        _PEER_INFO_TTL_SECONDS,
        lambda: get_peer_info(peers),
    )


def _get_peer_metric_averages_cached(peers: list, max_workers: Optional[int] = None) -> dict:
    peers_tuple = _peers_key(peers)
    key = ("peer_avgs", peers_tuple)
    return _cache_get_or_build(
        key,
        _PEER_AVG_TTL_SECONDS,
        lambda: get_peer_metric_averages(peers, max_workers=max_workers),
    )


def _clear_summary_caches_for_tests():
    with _CACHE_LOCK:
        _SUMMARY_CACHE.clear()


def get_summary_overview(symbol: str) -> dict:
    symbol = normalize_symbol(symbol)

    with ThreadPoolExecutor(max_workers=2) as executor:
        fundamentals_future = executor.submit(_get_fundamentals_cached, symbol)
        peers_future = executor.submit(_get_peers_cached, symbol)

        fundamentals = fundamentals_future.result()
        peers = peers_future.result()

    peers = normalize_peers(symbol, peers, max_peers=_PEER_AVG_MAX_PEERS)
    if peers:
        peer_avgs = _get_peer_metric_averages_cached(peers)
    else:
        peer_avgs = {f"avg_peer_{metric}": None for metric in PEER_METRICS}

    return {
        "symbol": symbol,
        "trailingPE": fundamentals.get("trailingPE"),
        "forwardPE": fundamentals.get("forwardPE"),
        "PEG": fundamentals.get("PEG"),
        "PGI": fundamentals.get("PGI"),
        "dividendYield": fundamentals.get("dividendYield"),
        "beta": fundamentals.get("beta"),
        "marketCap": fundamentals.get("marketCap"),
        "avg_peer_trailingPE": peer_avgs.get("avg_peer_trailingPE"),
        "avg_peer_forwardPE": peer_avgs.get("avg_peer_forwardPE"),
        "avg_peer_PEG": peer_avgs.get("avg_peer_PEG"),
        "avg_peer_PGI": peer_avgs.get("avg_peer_PGI"),
        "avg_peer_beta": peer_avgs.get("avg_peer_beta"),
    }


def get_summary_peers(symbol: str) -> dict:
    symbol = normalize_symbol(symbol)
    peers = _get_peers_cached(symbol)
    peers = normalize_peers(symbol, peers, max_peers=_PEER_AVG_MAX_PEERS)
    peer_info = _get_peer_info_cached(peers)
    ordered_peers = [p for p in peers if peer_info.get(p, {}).get("latest_price") is not None]
    ordered_peers = ordered_peers[:_MAX_PEERS]
    peer_info = {peer: peer_info[peer] for peer in ordered_peers}
    return {
        "symbol": symbol,
        "peer_info": peer_info,
    }


def get_summary_fundamentals(symbol: str) -> dict:
    symbol = normalize_symbol(symbol)
    fundamentals = _get_fundamentals_cached(symbol)
    return {
        "symbol": symbol,
        "trailingPE": fundamentals.get("trailingPE"),
        "forwardPE": fundamentals.get("forwardPE"),
        "PEG": fundamentals.get("PEG"),
        "PGI": fundamentals.get("PGI"),
        "dividendYield": fundamentals.get("dividendYield"),
        "beta": fundamentals.get("beta"),
        "marketCap": fundamentals.get("marketCap"),
        "revenuePerEmployee": fundamentals.get("revenuePerEmployee"),
        "grossProfitPerEmployee": fundamentals.get("grossProfitPerEmployee"),
        "operatingIncomePerEmployee": fundamentals.get("operatingIncomePerEmployee"),
        "sgaPerEmployee": fundamentals.get("sgaPerEmployee"),
        "salesPerSalesperson": fundamentals.get("salesPerSalesperson"),
        "roic": fundamentals.get("roic"),
        "roa": fundamentals.get("roa"),
        "assetTurnover": fundamentals.get("assetTurnover"),
        "capexIntensity": fundamentals.get("capexIntensity"),
        "freeCashFlowMargin": fundamentals.get("freeCashFlowMargin"),
        "grossMargin": fundamentals.get("grossMargin"),
        "operatingMargin": fundamentals.get("operatingMargin"),
        "sgaPercentRevenue": fundamentals.get("sgaPercentRevenue"),
        "rdPercentRevenue": fundamentals.get("rdPercentRevenue"),
        "metricTrends": fundamentals.get("metricTrends"),
    }


def get_summary_peer_averages(symbol: str) -> dict:
    symbol = normalize_symbol(symbol)

    def _compute():
        peers = normalize_peers(
            symbol,
            _get_peers_cached(symbol),
            max_peers=_PEER_AVG_MAX_PEERS,
        )
        if not peers:
            return {f"avg_peer_{metric}": None for metric in PEER_METRICS}
        return _get_peer_metric_averages_cached(peers)

    peer_avgs = _compute()
    return {
        "symbol": symbol,
        "avg_peer_trailingPE": peer_avgs.get("avg_peer_trailingPE"),
        "avg_peer_forwardPE": peer_avgs.get("avg_peer_forwardPE"),
        "avg_peer_PEG": peer_avgs.get("avg_peer_PEG"),
        "avg_peer_PGI": peer_avgs.get("avg_peer_PGI"),
        "avg_peer_beta": peer_avgs.get("avg_peer_beta"),
    }


def get_summary_bundle(symbol: str) -> dict:
    symbol = normalize_symbol(symbol)
    with ThreadPoolExecutor(max_workers=3) as executor:
        summary_future = executor.submit(_get_summary_cached, symbol)
        peers_future = executor.submit(_get_peers_cached, symbol)
        fundamentals_future = executor.submit(_get_fundamentals_cached, symbol)

        summary = summary_future.result()
        peers_raw = peers_future.result()
        fundamentals = fundamentals_future.result()

    peers = normalize_peers(symbol, peers_raw, max_peers=_PEER_AVG_MAX_PEERS)

    with ThreadPoolExecutor(max_workers=2) as executor:
        peer_info_future = executor.submit(_get_peer_info_cached, peers[:_MAX_PEERS])
        peer_avgs_future = executor.submit(_get_peer_metric_averages_cached, peers, 3)
        peer_info = peer_info_future.result()
        peer_avgs = peer_avgs_future.result()

    return {
        **summary,
        "peer_info": peer_info,
        "trailingPE": fundamentals.get("trailingPE"),
        "forwardPE": fundamentals.get("forwardPE"),
        "PEG": fundamentals.get("PEG"),
        "PGI": fundamentals.get("PGI"),
        "dividendYield": fundamentals.get("dividendYield"),
        "beta": fundamentals.get("beta"),
        "marketCap": fundamentals.get("marketCap"),
        "revenuePerEmployee": fundamentals.get("revenuePerEmployee"),
        "grossProfitPerEmployee": fundamentals.get("grossProfitPerEmployee"),
        "operatingIncomePerEmployee": fundamentals.get("operatingIncomePerEmployee"),
        "sgaPerEmployee": fundamentals.get("sgaPerEmployee"),
        "salesPerSalesperson": fundamentals.get("salesPerSalesperson"),
        "roic": fundamentals.get("roic"),
        "roa": fundamentals.get("roa"),
        "assetTurnover": fundamentals.get("assetTurnover"),
        "capexIntensity": fundamentals.get("capexIntensity"),
        "freeCashFlowMargin": fundamentals.get("freeCashFlowMargin"),
        "grossMargin": fundamentals.get("grossMargin"),
        "operatingMargin": fundamentals.get("operatingMargin"),
        "sgaPercentRevenue": fundamentals.get("sgaPercentRevenue"),
        "rdPercentRevenue": fundamentals.get("rdPercentRevenue"),
        "metricTrends": fundamentals.get("metricTrends"),
        "avg_peer_trailingPE": peer_avgs.get("avg_peer_trailingPE"),
        "avg_peer_forwardPE": peer_avgs.get("avg_peer_forwardPE"),
        "avg_peer_PEG": peer_avgs.get("avg_peer_PEG"),
        "avg_peer_PGI": peer_avgs.get("avg_peer_PGI"),
        "avg_peer_beta": peer_avgs.get("avg_peer_beta"),
    }
