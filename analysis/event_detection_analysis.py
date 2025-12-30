"""
event_detection_analysis.py
Purpose: turn touch/hug events into bounce/pullback stats.
Pseudocode:
1) Build touches and hugs from Bollinger data.
2) For each touch/hug, find the next local high/low in a lookahead window.
3) Return structured lists for downstream aggregation.
"""
from .event_detection import (
    process_bollinger_touches,
    detect_hug_events,
    find_short_term_high,
    find_short_term_low,
)

def get_touch_and_hug_events(data, include_hugs: bool = True):
    """Return touches and optional hug events for one symbol DataFrame."""
    touches = process_bollinger_touches(data, mode='historical')
    if not include_hugs:
        return touches, [], []
    hug_events_upper, hug_events_lower = detect_hug_events(data, touches)

    return touches, hug_events_upper, hug_events_lower

def _touch_event(data, touch, pivot_idx, pivot_price, is_lower: bool) -> dict:
    close_at_touch = data.loc[touch["index"], "close"]
    days_diff = pivot_idx - touch["index"]
    if is_lower:
        return {
            "touch_date": touch["date"],
            "touch_price": touch["price"],
            "peak_date": data.loc[pivot_idx, "date"],
            "peak_price": pivot_price,
            "bounce_dollars": pivot_price - close_at_touch,
            "trading_days": days_diff,
        }
    return {
        "touch_date": touch["date"],
        "touch_price": touch["price"],
        "trough_date": data.loc[pivot_idx, "date"],
        "trough_price": pivot_price,
        "drop_dollars": -(close_at_touch - pivot_price),
        "trading_days": days_diff,
    }


def _hug_event(data, hug, pivot_idx, pivot_price, is_lower: bool) -> dict:
    close_at_end = data.loc[hug["end_index"], "close"]
    days_diff = pivot_idx - hug["end_index"]
    base = {
        "hug_start_date": hug["start_date"],
        "hug_end_date": hug["end_date"],
        "hug_start_price": hug["start_price"],
        "hug_end_price": hug["end_price"],
        "intra_hug_change": hug["end_price"] - hug["start_price"],
        "trading_days": days_diff,
    }
    if is_lower:
        base.update({
            "peak_date": data.loc[pivot_idx, "date"],
            "peak_price": pivot_price,
            "bounce_dollars": pivot_price - close_at_end,
        })
    else:
        base.update({
            "trough_date": data.loc[pivot_idx, "date"],
            "trough_price": pivot_price,
            "drop_dollars": -(close_at_end - pivot_price),
        })
    return base


def compute_bounces_and_pullbacks(data, touches, hug_events_upper, hug_events_lower, window: int) -> dict:
    """Compute bounces/pullbacks for touches and hugs."""
    lower_touch_bounces = []
    upper_touch_pullbacks = []
    lower_hug_bounces = []
    upper_hug_pullbacks = []

    for t in touches:
        if t['band'] == 'lower':
            pivot_idx, pivot_price = find_short_term_high(data, t['index'] + 1, window=window)
            if pivot_idx is not None:
                lower_touch_bounces.append(_touch_event(data, t, pivot_idx, pivot_price, True))
        else:
            pivot_idx, pivot_price = find_short_term_low(data, t['index'] + 1, window=window)
            if pivot_idx is not None:
                upper_touch_pullbacks.append(_touch_event(data, t, pivot_idx, pivot_price, False))

    for hug in hug_events_lower:
        pivot_idx, pivot_price = find_short_term_high(data, hug['end_index'] + 1, window=window)
        if pivot_idx is not None:
            lower_hug_bounces.append(_hug_event(data, hug, pivot_idx, pivot_price, True))

    for hug in hug_events_upper:
        pivot_idx, pivot_price = find_short_term_low(data, hug['end_index'] + 1, window=window)
        if pivot_idx is not None:
            upper_hug_pullbacks.append(_hug_event(data, hug, pivot_idx, pivot_price, False))

    return {
        'lower_touch_bounces': lower_touch_bounces,
        'upper_touch_pullbacks': upper_touch_pullbacks,
        'lower_hug_bounces': lower_hug_bounces,
        'upper_hug_pullbacks': upper_hug_pullbacks,
    }
