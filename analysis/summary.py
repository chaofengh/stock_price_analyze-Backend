# summary.py

from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from .data_preparation import prepare_stock_data
from .event_detection_analysis import get_touch_and_hug_events, compute_bounces_and_pullbacks
from .metrics_calculation import compute_aggregates
from .chart_builder import build_chart_data
from .fundamentals import (
    get_fundamentals,
    get_peers,
)
from .data_fetcher import fetch_stock_data
from utils.ttl_cache import TTLCache

_MAX_PEERS = 6
_PEER_AVG_MAX_PEERS = 12
_FUNDAMENTALS_CACHE = TTLCache(ttl_seconds=60 * 60 * 12, max_size=512)
_FUNDAMENTALS_EMPTY_CACHE = TTLCache(ttl_seconds=60 * 5, max_size=512)
_PEERS_CACHE = TTLCache(ttl_seconds=60 * 60 * 12, max_size=512)
_PEER_FUNDAMENTALS_CACHE = TTLCache(ttl_seconds=60 * 60 * 12, max_size=2048)
_PEER_FUNDAMENTALS_EMPTY_CACHE = TTLCache(ttl_seconds=60 * 5, max_size=2048)
_PEER_INFO_CACHE = TTLCache(ttl_seconds=60 * 5, max_size=512)
_PEER_METRICS = ("trailingPE", "forwardPE", "PEG", "PGI", "beta")
_PEER_AVG_CACHE = TTLCache(ttl_seconds=60 * 10, max_size=512)
_NO_DATA = object()


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper() if symbol else ""


def _get_cached_fundamentals(symbol: str) -> dict:
    sym = _normalize_symbol(symbol)
    cached = _FUNDAMENTALS_CACHE.get(sym, _NO_DATA)
    if cached is not _NO_DATA:
        return cached
    cached_empty = _FUNDAMENTALS_EMPTY_CACHE.get(sym, _NO_DATA)
    if cached_empty is not _NO_DATA:
        return cached_empty
    fundamentals = get_fundamentals(sym)
    if fundamentals and any(val is not None for val in fundamentals.values()):
        _FUNDAMENTALS_CACHE.set(sym, fundamentals)
    else:
        _FUNDAMENTALS_EMPTY_CACHE.set(sym, fundamentals)
    return fundamentals


def _get_cached_peers(symbol: str) -> list:
    sym = _normalize_symbol(symbol)
    return _PEERS_CACHE.get_or_set(sym, lambda: get_peers(sym))


def _get_cached_peer_fundamentals(symbol: str) -> dict:
    sym = _normalize_symbol(symbol)
    cached = _PEER_FUNDAMENTALS_CACHE.get(sym, _NO_DATA)
    if cached is not _NO_DATA:
        return cached
    cached_empty = _PEER_FUNDAMENTALS_EMPTY_CACHE.get(sym, _NO_DATA)
    if cached_empty is not _NO_DATA:
        return cached_empty
    fundamentals = get_fundamentals(sym)
    if fundamentals and any(val is not None for val in fundamentals.values()):
        _PEER_FUNDAMENTALS_CACHE.set(sym, fundamentals)
    else:
        _PEER_FUNDAMENTALS_EMPTY_CACHE.set(sym, fundamentals)
    return fundamentals


def _normalize_peers(symbol: str, peers: list, max_peers: int = _MAX_PEERS) -> list:
    normalized_symbol = _normalize_symbol(symbol)
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


def _build_peer_info(peers: list) -> dict:
    peer_info = {}
    if not peers:
        return peer_info

    peer_data = fetch_stock_data(peers, period="1d", interval="5m")
    missing_peers = []

    for peer_symbol in peers:
        df_peer = peer_data.get(peer_symbol, pd.DataFrame())
        if df_peer.empty:
            missing_peers.append(peer_symbol)
            continue

        first_close = df_peer.iloc[0]['close']
        last_close = df_peer.iloc[-1]['close']
        if first_close is None or pd.isna(first_close) or first_close == 0:
            missing_peers.append(peer_symbol)
            continue
        pct_change = ((last_close - first_close) / first_close) * 100

        intraday_close_5m = [
            {'close': float(price)}
            for price in df_peer['close'].tail(50).tolist()
        ]

        peer_info[peer_symbol] = {
            'latest_price': last_close,
            'percentage_change': pct_change,
            'intraday_close_5m': intraday_close_5m
        }

    if missing_peers:
        daily_data = fetch_stock_data(missing_peers, period="5d", interval="1d")
        for peer_symbol in missing_peers:
            df_daily = daily_data.get(peer_symbol, pd.DataFrame())
            if df_daily.empty:
                peer_info[peer_symbol] = {
                    'latest_price': None,
                    'percentage_change': None,
                    'intraday_close_5m': []
                }
                continue

            first_close = df_daily.iloc[0]['close']
            last_close = df_daily.iloc[-1]['close']
            if first_close is None or pd.isna(first_close) or first_close == 0:
                pct_change = None
            else:
                pct_change = ((last_close - first_close) / first_close) * 100

            daily_close = [
                {'close': float(price)}
                for price in df_daily['close'].tail(50).tolist()
            ]

            peer_info[peer_symbol] = {
                'latest_price': last_close,
                'percentage_change': pct_change,
                'intraday_close_5m': daily_close
            }

    return peer_info


def _get_cached_peer_info(peers: list) -> dict:
    if not peers:
        return {}
    key = tuple(peers)
    return _PEER_INFO_CACHE.get_or_set(key, lambda: _build_peer_info(peers))


def _get_peer_metric_averages(peers: list) -> dict:
    totals = {metric: 0.0 for metric in _PEER_METRICS}
    counts = {metric: 0 for metric in _PEER_METRICS}
    if not peers:
        return {f"avg_peer_{metric}": None for metric in _PEER_METRICS}

    max_workers = min(8, len(peers))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_get_cached_peer_fundamentals, peer): peer for peer in peers
        }
        for future in as_completed(futures):
            try:
                fundamentals = future.result()
            except Exception:
                continue
            for metric in _PEER_METRICS:
                val = fundamentals.get(metric)
                if val is not None:
                    totals[metric] += val
                    counts[metric] += 1

    return {
        f"avg_peer_{metric}": (totals[metric] / counts[metric] if counts[metric] else None)
        for metric in _PEER_METRICS
    }

def get_summary_overview(symbol: str) -> dict:
    symbol = _normalize_symbol(symbol)

    with ThreadPoolExecutor(max_workers=2) as executor:
        fundamentals_future = executor.submit(_get_cached_fundamentals, symbol)
        peers_future = executor.submit(_get_cached_peers, symbol)

        fundamentals = fundamentals_future.result()
        peers = peers_future.result()

    peers = _normalize_peers(symbol, peers, max_peers=_PEER_AVG_MAX_PEERS)
    if peers:
        peer_avgs = _get_peer_metric_averages(peers)
    else:
        peer_avgs = {f"avg_peer_{metric}": None for metric in _PEER_METRICS}

    return {
        'symbol': symbol,
        'trailingPE': fundamentals.get("trailingPE"),
        'forwardPE': fundamentals.get("forwardPE"),
        'PEG': fundamentals.get("PEG"),
        'PGI': fundamentals.get("PGI"),
        'dividendYield': fundamentals.get("dividendYield"),
        'beta': fundamentals.get("beta"),
        'marketCap': fundamentals.get("marketCap"),
        'avg_peer_trailingPE': peer_avgs.get("avg_peer_trailingPE"),
        'avg_peer_forwardPE': peer_avgs.get("avg_peer_forwardPE"),
        'avg_peer_PEG': peer_avgs.get("avg_peer_PEG"),
        'avg_peer_PGI': peer_avgs.get("avg_peer_PGI"),
        'avg_peer_beta': peer_avgs.get("avg_peer_beta"),
    }


def get_summary_peers(symbol: str) -> dict:
    symbol = _normalize_symbol(symbol)
    peers = _get_cached_peers(symbol)
    peers = _normalize_peers(symbol, peers, max_peers=_MAX_PEERS)
    peer_info = _get_cached_peer_info(peers)
    return {
        'symbol': symbol,
        'peer_info': peer_info,
    }


def get_summary_fundamentals(symbol: str) -> dict:
    symbol = _normalize_symbol(symbol)
    fundamentals = _get_cached_fundamentals(symbol)
    return {
        'symbol': symbol,
        'trailingPE': fundamentals.get("trailingPE"),
        'forwardPE': fundamentals.get("forwardPE"),
        'PEG': fundamentals.get("PEG"),
        'PGI': fundamentals.get("PGI"),
        'dividendYield': fundamentals.get("dividendYield"),
        'beta': fundamentals.get("beta"),
        'marketCap': fundamentals.get("marketCap"),
    }


def get_summary_peer_averages(symbol: str) -> dict:
    symbol = _normalize_symbol(symbol)

    def _compute():
        peers = _normalize_peers(
            symbol,
            _get_cached_peers(symbol),
            max_peers=_PEER_AVG_MAX_PEERS,
        )
        if not peers:
            return {f"avg_peer_{metric}": None for metric in _PEER_METRICS}
        return _get_peer_metric_averages(peers)

    peer_avgs = _PEER_AVG_CACHE.get_or_set(symbol, _compute)
    return {
        'symbol': symbol,
        'avg_peer_trailingPE': peer_avgs.get("avg_peer_trailingPE"),
        'avg_peer_forwardPE': peer_avgs.get("avg_peer_forwardPE"),
        'avg_peer_PEG': peer_avgs.get("avg_peer_PEG"),
        'avg_peer_PGI': peer_avgs.get("avg_peer_PGI"),
        'avg_peer_beta': peer_avgs.get("avg_peer_beta"),
    }

def get_summary(symbol: str) -> dict:
    symbol = _normalize_symbol(symbol)
    # 1. Fetch and prepare data
    data_dict = prepare_stock_data(symbol, include_rsi=False)
    if symbol not in data_dict:
        raise ValueError(f"No data found for symbol {symbol}")
    df = data_dict[symbol]
    if df.empty:
        raise ValueError(f"No data found for symbol {symbol}")
    n = len(df)
    initial_price = float(df.loc[0, 'close'])
    final_price = float(df.loc[n - 1, 'close'])
    
    # 2. Detect touches & hug events
    touches, hug_events_upper, hug_events_lower = get_touch_and_hug_events(
        df,
        include_hugs=False,
    )
    
    # 3. Compute bounce/pullback events
    results_window_5 = compute_bounces_and_pullbacks(df, touches, hug_events_upper, hug_events_lower, window=5)
    results_window_10 = compute_bounces_and_pullbacks(df, touches, hug_events_upper, hug_events_lower, window=10)
    
    # 4. Compute aggregated metrics
    aggregated_window_5 = compute_aggregates(results_window_5, hug_events_upper, hug_events_lower)
    aggregated_window_10 = compute_aggregates(results_window_10, hug_events_upper, hug_events_lower)
    
    # 5. Build chart data
    chart_data = build_chart_data(df, touches)
    
    # 6. Compile the summary dictionary (lean)
    def _trim_window(results: dict) -> dict:
        return {
            'lower_touch_bounces': results.get('lower_touch_bounces', []),
            'upper_touch_pullbacks': results.get('upper_touch_pullbacks', []),
        }

    def _trim_aggregates(aggregates: dict) -> dict:
        keys = [
            'upper_touch_accuracy',
            'avg_upper_touch_drop',
            'upper_touch_count',
            'avg_upper_touch_in_days',
            'lower_touch_accuracy',
            'avg_lower_touch_bounce',
            'lower_touch_count',
            'avg_lower_touch_bounce_in_days',
        ]
        return {key: aggregates.get(key) for key in keys}

    summary = {
        'symbol': symbol,
        'final_price': final_price,
        'price_change_in_dollars': final_price - initial_price,

        'chart_data': chart_data,

        'window_5': _trim_window(results_window_5),
        'window_10': _trim_window(results_window_10),

        'aggregated_window_5': _trim_aggregates(aggregated_window_5),
        'aggregated_window_10': _trim_aggregates(aggregated_window_10),
    }

    return summary
