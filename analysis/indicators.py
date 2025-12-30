"""
indicators.py
Purpose: calculate technical indicators used across the app.
Pseudocode:
1) Use TA-Lib for Bollinger Bands and RSI.
2) Optionally compute basic support/resistance levels for intraday data.
"""
import talib
import pandas as pd
import numpy as np

def compute_bollinger_bands(
    data: pd.DataFrame,
    timeperiod: int = 20,
    nbdevup: int = 2,
    nbdevdn: int = 2,
) -> pd.DataFrame:
    """Add Bollinger Bands columns to a price DataFrame."""
    upper, mid, lower = talib.BBANDS(
        data['close'].astype(float),
        timeperiod=timeperiod, nbdevup=nbdevup, nbdevdn=nbdevdn, matype=0
    )
    data['BB_upper'] = upper
    data['BB_middle'] = mid
    data['BB_lower'] = lower
    return data

def compute_rsi(data: pd.DataFrame, timeperiod: int = 6) -> pd.DataFrame:
    """Add RSI column to a price DataFrame."""
    data['rsi'] = talib.RSI(data['close'], timeperiod=timeperiod)
    return data

def compute_realtime_sr(
    df: pd.DataFrame,
    *,
    window: int = 15,            # bars to look back (incl. current)
    tolerance_pct: float = 0.0025,  # 0.25% of price
    min_bounces: int = 3
) -> pd.DataFrame:
    """
    Add support/resistance columns based on recent price clusters.

    Support: closes cluster within tolerance and price dips below the cluster.
    Resistance: closes cluster within tolerance and price pokes above the cluster.
    """
    df = df.copy()
    n   = len(df)
    sup = np.full(n, np.nan, dtype=float)
    res = np.full(n, np.nan, dtype=float)

    close = df["close"].values
    low   = df["low"].values
    high  = df["high"].values

    def _scan(level_side: str, idx: int) -> float:
        """Scan recent bars and return the latest level that meets touch rules."""
        start   = max(0, idx - window + 1)
        c_now   = close[idx]
        tol     = c_now * tolerance_pct            # 0.25% in dollars
        buckets: dict[int, tuple[list[float], int]] = {}
        for c, lo, hi in zip(close[start:idx + 1],
                             low[start:idx + 1],
                             high[start:idx + 1]):
            # bucket width also = tol so we group prices that are
            # within +/- tol of each other
            key = int(round(c / tol))
            prices, touches = buckets.get(key, ([], 0))
            prices.append(c)

            if level_side == "support":
                if lo < (key * tol) - tol:         # dipped below cluster floor
                    touches += 1
            else:  # resistance
                if hi > (key * tol) + tol:         # poked above cluster ceiling
                    touches += 1

            buckets[key] = (prices, touches)

        level_price = np.nan
        for key, (prices, touches) in buckets.items():
            if touches >= min_bounces:
                level_price = np.mean(prices)
        return level_price

    for i in range(n):
        sup[i] = _scan("support",    i)
        res[i] = _scan("resistance", i)

    df["support"]    = sup
    df["resistance"] = res
    return df
