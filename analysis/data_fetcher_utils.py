import numpy as np
import pandas as pd


def normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper() if symbol else ""


def symbol_candidates(symbol: str) -> list:
    sym = normalize_symbol(symbol)
    if not sym:
        return []
    candidates = [sym]
    if "-" in sym:
        candidates.append(sym.replace("-", "."))
    if "." in sym:
        candidates.append(sym.replace(".", "-"))
    if "/" in sym:
        candidates.append(sym.replace("/", "-"))
    seen = set()
    unique = []
    for candidate in candidates:
        if candidate and candidate not in seen:
            seen.add(candidate)
            unique.append(candidate)
    return unique


def safe_float(value):
    if value is None:
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(value):
        return None
    return value


def get_info_value(info, keys):
    if not isinstance(info, dict):
        return None
    if isinstance(keys, str):
        keys = [keys]
    for key in keys:
        if key in info:
            value = info.get(key)
            if value is not None:
                return value
    return None


def get_ticker_info(ticker):
    info = {}
    try:
        if hasattr(ticker, "get_info"):
            info = ticker.get_info() or {}
    except Exception:
        info = {}
    if not info:
        try:
            info = ticker.info or {}
        except Exception:
            info = {}
    return info if isinstance(info, dict) else {}


def get_fast_info(ticker):
    fast_info = {}
    try:
        if hasattr(ticker, "get_fast_info"):
            fast_info = ticker.get_fast_info() or {}
        else:
            fast_info = ticker.fast_info or {}
    except Exception:
        fast_info = {}
    return fast_info if isinstance(fast_info, dict) else {}


def get_price_from_history(ticker):
    try:
        hist = ticker.history(period="1d", interval="1d")
    except Exception:
        return None
    if hist is None or getattr(hist, "empty", True):
        return None
    for col in ("Close", "Adj Close"):
        if col in hist.columns:
            value = safe_float(hist[col].iloc[-1])
            if value is not None:
                return value
    return None
