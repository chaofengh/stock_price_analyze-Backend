"""
world_markets.py
Purpose: build a world market snapshot with percent changes.
Pseudocode:
1) Bulk download recent closes for world indices.
2) Fallback to per-ticker history if bulk data is missing.
3) Return a normalized list of market entries.
"""
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

import pandas as pd
import yfinance as yf

WORLD_MARKETS = [
    {
        "id": "DOW",
        "label": "DOW",
        "ticker": "^DJI",
        "name": "Dow Jones Industrial Average",
    },
    {
        "id": "CADOW",
        "label": "CADOW",
        "ticker": "^GSPTSE",
        "name": "S&P/TSX Composite",
    },
    {
        "id": "MXDOW",
        "label": "MXDOW",
        "ticker": "^MXX",
        "name": "S&P/BMV IPC",
    },
    {
        "id": "FTSE",
        "label": "FTSE",
        "ticker": "^FTSE",
        "name": "FTSE 100",
    },
    {
        "id": "FRDOW",
        "label": "FRDOW",
        "ticker": "^FCHI",
        "name": "CAC 40",
    },
    {
        "id": "DEDOW",
        "label": "DEDOW",
        "ticker": "^GDAXI",
        "name": "DAX",
    },
    {
        "id": "ESDOW",
        "label": "ESDOW",
        "ticker": "^IBEX",
        "name": "IBEX 35",
    },
    {
        "id": "ITDOW",
        "label": "ITDOW",
        "ticker": "FTSEMIB.MI",
        "name": "FTSE MIB",
    },
    {
        "id": "HKDOW",
        "label": "HKDOW",
        "ticker": "^HSI",
        "name": "Hang Seng",
    },
    {
        "id": "SGDOW",
        "label": "SGDOW",
        "ticker": "^STI",
        "name": "Straits Times",
    },
    {
        "id": "N225",
        "label": "N225",
        "ticker": "^N225",
        "name": "Nikkei 225",
    },
    {
        "id": "DJAU",
        "label": "DJAU",
        "ticker": "^AXJO",
        "name": "S&P/ASX 200",
    },
    {
        "id": "NZDOW",
        "label": "NZDOW",
        "ticker": "^NZ50",
        "name": "NZX 50",
    },
]


def _safe_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_close_series(raw: pd.DataFrame, ticker: str) -> Optional[pd.Series]:
    if raw is None or raw.empty:
        return None
    if isinstance(raw.columns, pd.MultiIndex):
        level0 = raw.columns.get_level_values(0)
        level1 = raw.columns.get_level_values(1)
        data = None
        if ticker in level0:
            data = raw[ticker]
        elif ticker in level1:
            data = raw.xs(ticker, axis=1, level=1)
        else:
            alt = ticker.lstrip("^")
            if alt in level0:
                data = raw[alt]
            elif alt in level1:
                data = raw.xs(alt, axis=1, level=1)
            else:
                return None
    else:
        data = raw
    if isinstance(data, pd.Series):
        series = data.dropna()
    elif "Close" in data.columns:
        series = data["Close"].dropna()
    elif "Adj Close" in data.columns:
        series = data["Adj Close"].dropna()
    else:
        return None
    return series if not series.empty else None


def _fetch_fallback_series(ticker: str) -> Optional[pd.Series]:
    try:
        hist = yf.Ticker(ticker).history(period="7d", interval="1d", auto_adjust=False)
    except Exception:
        return None
    if hist is None or getattr(hist, "empty", True):
        return None
    if "Close" in hist.columns:
        series = hist["Close"].dropna()
    elif "Adj Close" in hist.columns:
        series = hist["Adj Close"].dropna()
    else:
        return None
    return series if not series.empty else None


def _resolve_series(raw: pd.DataFrame, ticker: str) -> Tuple[Optional[pd.Series], bool]:
    series = _extract_close_series(raw, ticker)
    if series is not None and len(series) >= 2:
        return series, False
    fallback = _fetch_fallback_series(ticker)
    if fallback is not None and len(fallback) >= 1:
        return fallback, True
    return series, False


def _build_market_entry(market: Dict[str, str], raw: pd.DataFrame) -> Dict[str, object]:
    series, used_fallback = _resolve_series(raw, market["ticker"])
    last_close = None
    previous_close = None
    percent_change = None
    last_close_at = None

    if series is not None and len(series) >= 1:
        last_close = _safe_float(series.iloc[-1])
        last_timestamp = series.index[-1]
        if isinstance(last_timestamp, pd.Timestamp):
            last_close_at = last_timestamp.isoformat()
        else:
            try:
                last_close_at = pd.Timestamp(last_timestamp).isoformat()
            except (ValueError, TypeError):
                last_close_at = None

    if series is not None and len(series) >= 2:
        previous_close = _safe_float(series.iloc[-2])
        if last_close is not None and previous_close not in (None, 0):
            percent_change = (last_close - previous_close) / previous_close * 100

    return {
        "id": market["id"],
        "label": market["label"],
        "ticker": market["ticker"],
        "name": market["name"],
        "percent_change": percent_change,
        "last_close": last_close,
        "previous_close": previous_close,
        "last_close_at": last_close_at,
        "used_fallback": used_fallback,
    }


def fetch_world_market_moves() -> Dict[str, object]:
    tickers = " ".join(market["ticker"] for market in WORLD_MARKETS)
    raw = yf.download(
        tickers=tickers,
        period="7d",
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=False,
    )

    markets = [_build_market_entry(market, raw) for market in WORLD_MARKETS]
    as_of = datetime.now(timezone.utc).isoformat()
    return {
        "as_of": as_of,
        "source": "yfinance",
        "markets": markets,
    }
