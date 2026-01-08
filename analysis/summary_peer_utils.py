from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from utils.ttl_cache import TTLCache
from .data_fetcher import fetch_stock_data
from .summary_cache import normalize_symbol, get_cached_peer_fundamentals

PEER_METRICS = ("trailingPE", "forwardPE", "PEG", "PGI", "beta")
_PEER_INFO_CACHE = TTLCache(ttl_seconds=60 * 5, max_size=512)


def normalize_peers(symbol: str, peers: list, max_peers: int) -> list:
    normalized_symbol = normalize_symbol(symbol)
    peers_filtered = []
    seen = set()
    for peer in peers or []:
        if not peer:
            continue
        peer_upper = peer.upper()
        if peer_upper == normalized_symbol or peer_upper in seen:
            continue
        seen.add(peer_upper)
        peers_filtered.append(peer_upper)
        if len(peers_filtered) >= max_peers:
            break
    return peers_filtered


def _valid_close(value) -> bool:
    return value is not None and not pd.isna(value) and value != 0


def _build_peer_entry(df: pd.DataFrame, require_first: bool = True) -> dict | None:
    if df is None or df.empty or "close" not in df.columns:
        return None
    close_series = df["close"].dropna()
    if close_series.empty:
        return None
    first_close = close_series.iloc[0]
    last_close = close_series.iloc[-1]
    if not _valid_close(first_close):
        if require_first:
            return None
        pct_change = None
    else:
        pct_change = ((last_close - first_close) / first_close) * 100
    close_series = [
        {"close": float(price)}
        for price in df["close"].tail(50).tolist()
        if not pd.isna(price)
    ]
    return {
        "latest_price": last_close,
        "percentage_change": pct_change,
        "intraday_close_5m": close_series,
    }


def _build_peer_info(peers: list) -> dict:
    peer_info = {}
    if not peers:
        return peer_info

    peer_data = fetch_stock_data(peers, period="1d", interval="5m", require_ohlc=False)
    missing_peers = []

    for peer_symbol in peers:
        entry = _build_peer_entry(peer_data.get(peer_symbol), require_first=True)
        if entry is None:
            missing_peers.append(peer_symbol)
        else:
            peer_info[peer_symbol] = entry

    if missing_peers:
        daily_data = fetch_stock_data(missing_peers, period="5d", interval="1d", require_ohlc=False)
        for peer_symbol in missing_peers:
            entry = _build_peer_entry(daily_data.get(peer_symbol), require_first=False)
            if entry is None:
                peer_info[peer_symbol] = {
                    "latest_price": None,
                    "percentage_change": None,
                    "intraday_close_5m": [],
                }
            else:
                peer_info[peer_symbol] = entry

    return peer_info


def get_cached_peer_info(peers: list) -> dict:
    if not peers:
        return {}
    key = tuple(peers)
    return _PEER_INFO_CACHE.get_or_set(key, lambda: _build_peer_info(peers))


def get_peer_metric_averages(peers: list) -> dict:
    totals = {metric: 0.0 for metric in PEER_METRICS}
    counts = {metric: 0 for metric in PEER_METRICS}
    if not peers:
        return {f"avg_peer_{metric}": None for metric in PEER_METRICS}

    max_workers = min(8, len(peers))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(get_cached_peer_fundamentals, peer): peer for peer in peers
        }
        for future in as_completed(futures):
            try:
                fundamentals = future.result()
            except Exception:
                continue
            for metric in PEER_METRICS:
                val = fundamentals.get(metric)
                if val is not None:
                    totals[metric] += val
                    counts[metric] += 1

    return {
        f"avg_peer_{metric}": (totals[metric] / counts[metric] if counts[metric] else None)
        for metric in PEER_METRICS
    }
