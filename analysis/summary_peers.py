"""
summary_peers.py
Purpose: build fundamentals and peer comparison payloads for summary endpoints.
"""
import os
from concurrent.futures import ThreadPoolExecutor

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


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


_SUMMARY_INCLUDE_ALPHA_FUNDAMENTALS = _bool_env(
    "SUMMARY_INCLUDE_ALPHA_FUNDAMENTALS",
    False,
)


def _get_summary_fast(symbol: str) -> dict:
    return get_summary(symbol)


def _get_peers_fast(symbol: str) -> list:
    return get_peers(symbol)


def _get_fundamentals_fast(symbol: str) -> dict:
    return get_fundamentals(
        symbol,
        include_alpha=_SUMMARY_INCLUDE_ALPHA_FUNDAMENTALS,
    )


def _get_peer_info_fast(peers: list) -> dict:
    return get_peer_info(peers)


def _get_peer_metric_averages_fast(peers: list, max_workers=None) -> dict:
    return get_peer_metric_averages(peers, max_workers=max_workers)


def get_summary_overview(symbol: str) -> dict:
    symbol = normalize_symbol(symbol)

    with ThreadPoolExecutor(max_workers=2) as executor:
        fundamentals_future = executor.submit(_get_fundamentals_fast, symbol)
        peers_future = executor.submit(_get_peers_fast, symbol)

        fundamentals = fundamentals_future.result()
        peers = peers_future.result()

    peers = normalize_peers(symbol, peers, max_peers=_PEER_AVG_MAX_PEERS)
    if peers:
        peer_avgs = _get_peer_metric_averages_fast(peers)
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
    peers = _get_peers_fast(symbol)
    peers = normalize_peers(symbol, peers, max_peers=_PEER_AVG_MAX_PEERS)
    peer_info = _get_peer_info_fast(peers)
    ordered_peers = [p for p in peers if peer_info.get(p, {}).get("latest_price") is not None]
    ordered_peers = ordered_peers[:_MAX_PEERS]
    peer_info = {peer: peer_info[peer] for peer in ordered_peers}
    return {
        "symbol": symbol,
        "peer_info": peer_info,
    }


def get_summary_fundamentals(symbol: str) -> dict:
    symbol = normalize_symbol(symbol)
    fundamentals = _get_fundamentals_fast(symbol)
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

    peers = normalize_peers(
        symbol,
        _get_peers_fast(symbol),
        max_peers=_PEER_AVG_MAX_PEERS,
    )
    if not peers:
        peer_avgs = {f"avg_peer_{metric}": None for metric in PEER_METRICS}
    else:
        peer_avgs = _get_peer_metric_averages_fast(peers)

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
        summary_future = executor.submit(_get_summary_fast, symbol)
        peers_future = executor.submit(_get_peers_fast, symbol)
        fundamentals_future = executor.submit(_get_fundamentals_fast, symbol)

        summary = summary_future.result()
        peers_raw = peers_future.result()
        fundamentals = fundamentals_future.result()

    peers = normalize_peers(symbol, peers_raw, max_peers=_PEER_AVG_MAX_PEERS)

    with ThreadPoolExecutor(max_workers=2) as executor:
        peer_info_future = executor.submit(_get_peer_info_fast, peers[:_MAX_PEERS])
        peer_avgs_future = executor.submit(_get_peer_metric_averages_fast, peers, 3)
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
