"""
summary_peers.py
Purpose: build fundamentals and peer comparison payloads for summary endpoints.
"""
from concurrent.futures import ThreadPoolExecutor
from utils.ttl_cache import TTLCache
from .summary_cache import normalize_symbol, get_cached_fundamentals, get_cached_peers
from .summary_peer_utils import (
    PEER_METRICS,
    normalize_peers,
    get_cached_peer_info,
    get_peer_metric_averages,
)

_MAX_PEERS = 6
_PEER_AVG_MAX_PEERS = 12
_PEER_AVG_CACHE = TTLCache(ttl_seconds=60 * 10, max_size=512)
_NO_DATA = object()


def get_summary_overview(symbol: str) -> dict:
    symbol = normalize_symbol(symbol)

    with ThreadPoolExecutor(max_workers=2) as executor:
        fundamentals_future = executor.submit(get_cached_fundamentals, symbol)
        peers_future = executor.submit(get_cached_peers, symbol)

        fundamentals = fundamentals_future.result()
        peers = peers_future.result()

    peers = normalize_peers(symbol, peers, max_peers=_PEER_AVG_MAX_PEERS)
    if peers:
        peer_avgs = get_peer_metric_averages(peers)
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
    peers = get_cached_peers(symbol)
    peers = normalize_peers(symbol, peers, max_peers=_MAX_PEERS)
    peer_info = get_cached_peer_info(peers)
    return {
        "symbol": symbol,
        "peer_info": peer_info,
    }


def get_summary_fundamentals(symbol: str) -> dict:
    symbol = normalize_symbol(symbol)
    fundamentals = get_cached_fundamentals(symbol)
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
            get_cached_peers(symbol),
            max_peers=_PEER_AVG_MAX_PEERS,
        )
        if not peers:
            return {f"avg_peer_{metric}": None for metric in PEER_METRICS}
        return get_peer_metric_averages(peers)

    peer_avgs = _PEER_AVG_CACHE.get(symbol, _NO_DATA)
    if peer_avgs is _NO_DATA:
        peer_avgs = _compute()
        if any(peer_avgs.get(f"avg_peer_{metric}") is not None for metric in PEER_METRICS):
            _PEER_AVG_CACHE.set(symbol, peer_avgs)
    return {
        "symbol": symbol,
        "avg_peer_trailingPE": peer_avgs.get("avg_peer_trailingPE"),
        "avg_peer_forwardPE": peer_avgs.get("avg_peer_forwardPE"),
        "avg_peer_PEG": peer_avgs.get("avg_peer_PEG"),
        "avg_peer_PGI": peer_avgs.get("avg_peer_PGI"),
        "avg_peer_beta": peer_avgs.get("avg_peer_beta"),
    }
