"""
peer_data.py
~~~~~~~~~~~~
Utility functions for retrieving and packaging intraday peer data.

Currently exports:
    • get_peer_info(peers, period="1d", interval="5m")
        → { "PEER1": [ {"timestamp": "...", "close": ...}, ... ], ... }

The timestamps are returned in ISO-8601 strings so the entire structure
is JSON-serialisable with no custom encoders.
"""
from typing import Dict, List
import pandas as pd
from .data_fetcher import fetch_stock_data


def get_peer_info(
    peers: List[str],
    period: str = "1d",
    interval: str = "5m",
) -> Dict[str, List[dict]]:
    """
    Fetch every *interval* close price for each peer over *period* and
    return a dict keyed by symbol.

    Parameters
    ----------
    peers : list[str]
        Ticker symbols of peer companies.
    period : str, optional (default "1d")
        yfinance-style look-back window (e.g. "5d", "1mo").
    interval : str, optional (default "5m")
        Candle resolution (e.g. "5m", "15m").

    Returns
    -------
    dict
        {
            "AAPL": [
                {"timestamp": "2025-08-04T09:30:00-04:00", "close": 182.35},
                ...
            ],
            "MSFT": [...],
            ...
        }
        If no data is available for a peer, the value is an empty list.
    """
    peer_info: Dict[str, List[dict]] = {}

    if not peers:
        return peer_info  # nothing to do

    # Bulk-fetch data for efficiency
    peer_data = fetch_stock_data(peers, period=period, interval=interval)

    for symbol in peers:
        df: pd.DataFrame = peer_data.get(symbol, pd.DataFrame())

        if df.empty or "close" not in df.columns:
            peer_info[symbol] = []  # keep key for consistency
            continue

        # Format to list-of-dicts (JSON-friendly)
        records = (
            df.reset_index()                                # make index a column
              .loc[:, ["datetime", "close"]]                # keep what we need
              .assign(datetime=lambda d: d["datetime"].dt.isoformat())
              .rename(columns={"datetime": "timestamp"})
              .to_dict(orient="records")
        )

        peer_info[symbol] = records

    return peer_info
