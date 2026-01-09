"""
fundamentals.py
Purpose: thin wrappers around fundamentals and peers fetchers.
Pseudocode:
1) Fetch fundamentals safely (return {} on failure).
2) Fetch peers and peer fundamentals for comparison.
"""
from .data_fetcher import fetch_stock_fundamentals, fetch_peers
from .data_fetcher_fundamentals_extract import is_empty_fundamentals
from .data_fetcher_fundamentals_loader import load_fundamentals
from .data_fetcher_utils import normalize_symbol, symbol_candidates

def get_fundamentals(symbol: str, include_alpha: bool = True) -> dict:
    """Return full fundamentals dict or empty dict on error."""
    symbol = normalize_symbol(symbol)
    for candidate in symbol_candidates(symbol):
        fundamentals = load_fundamentals(candidate, include_alpha=include_alpha)
        if not is_empty_fundamentals(fundamentals):
            return fundamentals
    raise ValueError(f"No fundamentals returned for symbol {symbol}")


def get_fundamentals_light(symbol: str) -> dict:
    """Return valuation-only fundamentals dict or empty dict on error."""
    try:
        fundamentals = fetch_stock_fundamentals(symbol)
    except Exception:
        fundamentals = {}
    return fundamentals

def get_peers(symbol: str) -> list:
    """Return peer symbols for a ticker."""
    return fetch_peers(symbol)

def get_peers_fundamentals(peers: list) -> dict:
    """Fetch fundamentals for each peer symbol."""
    peers_fundamentals = {}
    for peer_symbol in peers:
        try:
            f = fetch_stock_fundamentals(peer_symbol)
            peers_fundamentals[peer_symbol] = f
        except Exception:
            peers_fundamentals[peer_symbol] = {}
    return peers_fundamentals

def compute_peer_metric_avg(peers_fundamentals: dict, metric: str):
    vals = []
    for f_dict in peers_fundamentals.values():
        val = f_dict.get(metric)
        if val is not None:
            vals.append(val)
    return sum(vals) / len(vals) if vals else None

def compare_metric(main_val, peer_avg, metric_name: str) -> str:
    if main_val is None or peer_avg is None:
        return f"No peer comparison available for {metric_name}."
    elif main_val > peer_avg:
        return f"{metric_name} is above peer average ({main_val:.2f} vs. {peer_avg:.2f})."
    else:
        return f"{metric_name} is below (or near) peer average ({main_val:.2f} vs. {peer_avg:.2f})."
