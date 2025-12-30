"""
event_detection.py
Purpose: detect Bollinger Band touches and build multi-day "hug" events.
Pseudocode:
1) For alert mode, check the latest candle for upper/lower band touches.
2) For historical mode, scan all rows and record touch events.
3) Group consecutive touches into hug events.
"""
import pandas as pd


def _list_tail(series: pd.Series, n: int = 30) -> list:
    """Return the last N values as floats/None (no NaNs)."""
    out = []
    for value in series.tail(n).tolist():
        if value is None or value != value:  # NaN != NaN
            out.append(None)
            continue
        try:
            out.append(float(value))
        except (TypeError, ValueError):
            out.append(None)
    return out


def _has_required_fields(row: pd.Series, fields: list[str]) -> bool:
    """True if all required fields are present and not NaN."""
    return all((row.get(field) is not None) and (row.get(field) == row.get(field)) for field in fields)


def process_bollinger_touches(data, mode: str = "alert") -> list[dict]:
    """
    Process Bollinger Band touches in two modes.

    alert: data is {symbol: DataFrame}, only the last row is checked.
    historical: data is a single DataFrame, all rows are scanned.
    """
    results: list[dict] = []

    if mode == "alert":
        for symbol, df in data.items():
            if df is None or df.empty:
                continue
            last_row = df.iloc[-1]
            required = ["close", "BB_upper", "BB_lower", "high", "low"]
            if not _has_required_fields(last_row, required):
                continue

            touched_side = None
            if last_row["high"] >= last_row["BB_upper"]:
                touched_side = "Upper"
            elif last_row["low"] <= last_row["BB_lower"]:
                touched_side = "Lower"

            if touched_side:
                results.append({
                    "symbol": symbol,
                    "close_price": float(last_row["close"]),
                    "bb_upper": float(last_row["BB_upper"]),
                    "bb_lower": float(last_row["BB_lower"]),
                    "low_price": float(last_row["low"]),
                    "high_price": float(last_row["high"]),
                    "touched_side": touched_side,
                    "recent_closes": _list_tail(df["close"]),
                    "recent_bb_upper": _list_tail(df["BB_upper"]),
                    "recent_bb_lower": _list_tail(df["BB_lower"]),
                })
        return results

    if mode == "historical":
        required = ["date", "close", "BB_upper", "BB_lower", "high", "low"]
        for i in range(len(data)):
            row = data.iloc[i]
            if not _has_required_fields(row, required):
                continue
            if row["high"] >= row["BB_upper"]:
                results.append({
                    "date": row["date"],
                    "index": i,
                    "band": "upper",
                    "price": float(row["close"]),
                })
            if row["low"] <= row["BB_lower"]:
                results.append({
                    "date": row["date"],
                    "index": i,
                    "band": "lower",
                    "price": float(row["close"]),
                })
        return results

    return results



def detect_hug_events(
    data: pd.DataFrame,
    touches: list,
    min_group_len: int = 2,   # require at least this many consecutive days for an event
) -> tuple[list[dict], list[dict]]:
    """
    Build multi-day "hug" events directly from the `touches` list.

    A hug event is defined as >= `min_group_len` consecutive trading days
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
