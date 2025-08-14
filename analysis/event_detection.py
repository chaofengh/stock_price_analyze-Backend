#event_detection.py
import pandas as pd

# Touch detection remains largely the same
def process_bollinger_touches(data, mode='alert'):
    """
    Process Bollinger Band touches in two modes.
    
    For mode 'alert':
      - Expects data as a dict {symbol: DataFrame}.
      - Checks the latest row for each symbol.
      - Returns a list of alert dictionaries including:
            "symbol", "close_price", "bb_upper", "bb_lower", "touched_side",
            "recent_closes", "recent_bb_upper", "recent_bb_lower".
    
    For mode 'historical':
      - Expects data as a DataFrame for a single ticker.
      - Iterates over all rows and returns a list of touch events with:
            "date", "index", "band" (either 'upper' or 'lower'), and "price".
    """
    def _tolist_30(series):
        # Convert to a Python list of length <= 30 with floats/None (no NaNs)
        out = []
        for v in series.tail(30).tolist():
            if v is None or v != v:   # handles None and NaN (NaN != NaN)
                out.append(None)
            else:
                try:
                    out.append(float(v))
                except (TypeError, ValueError):
                    out.append(None)
        return out

    results = []
    
    if mode == 'alert':
        # data is a dict of {symbol: DataFrame}
        for symbol, df in data.items():
            if df is None or df.empty:
                continue

            last_row = df.iloc[-1]
            required_fields = ['close', 'BB_upper', 'BB_lower', 'high', 'low']
            # Robust check for missing/NaN values
            if any((last_row.get(f) is None) or (last_row.get(f) != last_row.get(f)) for f in required_fields):
                continue

            touched_side = None
            if last_row['high'] >= last_row['BB_upper']:
                touched_side = "Upper"
            elif last_row['low'] <= last_row['BB_lower']:
                touched_side = "Lower"
            
            if touched_side:
                results.append({
                    "symbol": symbol,
                    "close_price": float(last_row['close']),
                    "bb_upper": float(last_row['BB_upper']),
                    "bb_lower": float(last_row['BB_lower']),
                    "low_price": float(last_row['low']),
                    "high_price": float(last_row['high']),
                    "touched_side": touched_side,
                    # For charting (aligned, JSON-safe lists)
                    "recent_closes": _tolist_30(df['close']),
                    "recent_bb_upper": _tolist_30(df['BB_upper']),
                    "recent_bb_lower": _tolist_30(df['BB_lower']),
                })
                
    elif mode == 'historical':
        # data is a DataFrame for a single ticker
        n = len(data)
        for i in range(n):
            row = data.iloc[i]
            required_fields = ['date', 'close', 'BB_upper', 'BB_lower', 'high', 'low']
            if any((row.get(f) is None) or (row.get(f) != row.get(f)) for f in required_fields):
                continue
            if row['high'] >= row['BB_upper']:
                results.append({
                    "date": row['date'],
                    "index": i,
                    "band": "upper",
                    "price": float(row['close'])
                })
            if row['low'] <= row['BB_lower']:
                results.append({
                    "date": row['date'],
                    "index": i,
                    "band": "lower",
                    "price": float(row['close'])
                })
    return results



def detect_hug_events(
    data: pd.DataFrame,
    touches: list,
    min_group_len: int = 2,   # require at least this many consecutive days for an event
) -> tuple[list[dict], list[dict]]:
    """
    Build multi-day “hug” events directly from the `touches` list.

    A hug event is defined as ≥ `min_group_len` **consecutive trading days**
    where each close was already flagged as touching a Bollinger band.

    Parameters
    ----------
    data : pd.DataFrame
        Original price/Bollinger-band frame (used only for look-ups).
    touches : list[dict]
        Output of `process_bollinger_touches`, containing *index*, *date*,
        *price* and which *band* was touched.
    min_group_len : int, default 2
        Minimum number of consecutive touches to qualify as one event.

    Returns
    -------
    hug_events_upper : list[dict]
    hug_events_lower : list[dict]
    """

    # --- organise touches by band & sort by index --------------------------------
    by_band = {"upper": [], "lower": []}
    for t in touches:
        by_band[t["band"]].append(t)

    for band in by_band:
        by_band[band].sort(key=lambda x: x["index"])

    # --- helper to collapse consecutive-index blocks into events ------------------
    def build_events(band: str, band_touches: list[dict]) -> list[dict]:
        events, group = [], []

        def flush():
            if len(group) >= min_group_len:
                events.append(
                    {
                        "band": band,
                        "start_index": group[0]["index"],
                        "end_index": group[-1]["index"],
                        "start_date": data.loc[group[0]["index"], "date"],
                        "end_date": data.loc[group[-1]["index"], "date"],
                        "start_price": float(group[0]["price"]),
                        "end_price": float(group[-1]["price"]),
                    }
                )

        for t in band_touches:
            if not group:
                group.append(t)
            elif t["index"] == group[-1]["index"] + 1:
                group.append(t)
            else:
                flush()
                group = [t]

        flush()  # catch the final block
        return events

    hug_events_upper = build_events("upper", by_band["upper"])
    hug_events_lower = build_events("lower", by_band["lower"])

    return hug_events_upper, hug_events_lower


# New short-term high/low using a fixed lookahead window
def find_short_term_high(df, start_idx, window=5):
    """
    Finds the highest closing price in the next `window` trading days.
    """
    end_idx = min(len(df), start_idx + window)
    window_slice = df.iloc[start_idx:end_idx]
    if window_slice.empty:
        return (None, None)
    highest_idx = window_slice['close'].idxmax()
    highest_price = window_slice['close'].max()
    return (highest_idx, highest_price)

def find_short_term_low(df, start_idx, window=5):
    """
    Finds the lowest closing price in the next `window` trading days.
    """
    end_idx = min(len(df), start_idx + window)
    window_slice = df.iloc[start_idx:end_idx]
    if window_slice.empty:
        return (None, None)
    lowest_idx = window_slice['close'].idxmin()
    lowest_price = window_slice['close'].min()
    return (lowest_idx, lowest_price)


