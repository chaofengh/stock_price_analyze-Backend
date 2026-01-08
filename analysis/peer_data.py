"""
peer_data.py
Purpose: build intraday peer close series for the UI.
Pseudocode:
1) Fetch price data for peers in bulk.
2) For each peer, map time -> close into JSON-friendly records.
3) Return a dict keyed by peer symbol.
"""
from __future__ import annotations

from typing import Dict, List
import pandas as pd
from .data_fetcher import fetch_stock_data


def get_peer_info(
    peers: List[str],
    period: str = "1d",
    interval: str = "5m",
) -> Dict[str, List[dict]]:
    """Return intraday close series for each peer."""
    peer_info: Dict[str, List[dict]] = {}

    if not peers:
        return peer_info  # nothing to do

    # Bulk-fetch data for efficiency
    peer_data = fetch_stock_data(peers, period=period, interval=interval, require_ohlc=False)

    def _time_column(df: pd.DataFrame) -> str | None:
        if "date" in df.columns:
            return "date"
        if "datetime" in df.columns:
            return "datetime"
        return None

    for symbol in peers:
        df: pd.DataFrame = peer_data.get(symbol, pd.DataFrame())

        time_col = _time_column(df)
        if df.empty or "close" not in df.columns or time_col is None:
            peer_info[symbol] = []  # keep key for consistency
            continue

        records = (
            df.loc[:, [time_col, "close"]]
              .assign(timestamp=lambda d: d[time_col].apply(
                  lambda v: v.isoformat() if hasattr(v, "isoformat") else str(v)
              ))
              .loc[:, ["timestamp", "close"]]
              .to_dict(orient="records")
        )

        peer_info[symbol] = records

    return peer_info
