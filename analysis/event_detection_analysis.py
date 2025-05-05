# event_detection_analysis.py
from .event_detection import process_bollinger_touches, detect_hug_events, find_short_term_high, find_short_term_low

def get_touch_and_hug_events(data):
    touches = process_bollinger_touches(data,mode='historical')
    hug_events_upper, hug_events_lower = detect_hug_events(data, touches)

    return touches, hug_events_upper, hug_events_lower

def compute_bounces_and_pullbacks(data, touches, hug_events_upper, hug_events_lower, window: int) -> dict:
    lower_touch_bounces = []
    upper_touch_pullbacks = []
    lower_hug_bounces = []
    upper_hug_pullbacks = []

    for t in touches:
        if t['band'] == 'lower':
            pivot_idx, pivot_price = find_short_term_high(data, t['index'] + 1, window=window)
            if pivot_idx is not None:
                bounce_dollars = pivot_price - data.loc[t['index'], 'close']
                days_diff = pivot_idx - t['index']
                lower_touch_bounces.append({
                    'touch_date':  t['date'],
                    'touch_price': t['price'],
                    'peak_date':   data.loc[pivot_idx, 'date'],
                    'peak_price':  pivot_price,
                    'bounce_dollars': bounce_dollars,
                    'trading_days': days_diff
                })
        else:
            pivot_idx, pivot_price = find_short_term_low(data, t['index'] + 1, window=window)
            if pivot_idx is not None:
                drop_dollars = data.loc[t['index'], 'close'] - pivot_price
                days_diff = pivot_idx - t['index']
                upper_touch_pullbacks.append({
                    'touch_date':  t['date'],
                    'touch_price': t['price'],
                    'trough_date': data.loc[pivot_idx, 'date'],
                    'trough_price': pivot_price,
                    'drop_dollars': -drop_dollars,
                    'trading_days': days_diff
                })

    for hug in hug_events_lower:
        pivot_idx, pivot_price = find_short_term_high(data, hug['end_index'] + 1, window=window)
        if pivot_idx is not None:
            bounce_dollars = pivot_price - data.loc[hug['end_index'], 'close']
            days_diff = pivot_idx - hug['end_index']
            lower_hug_bounces.append({
                'hug_start_date': hug['start_date'],
                'hug_end_date': hug['end_date'],
                'hug_start_price': hug['start_price'],
                'hug_end_price': hug['end_price'],
                'intra_hug_change': hug['end_price'] - hug['start_price'],
                'peak_date': data.loc[pivot_idx, 'date'],
                'peak_price': pivot_price,
                'bounce_dollars': bounce_dollars,
                'trading_days': days_diff
            })

    for hug in hug_events_upper:
        pivot_idx, pivot_price = find_short_term_low(data, hug['end_index'] + 1, window=window)
        if pivot_idx is not None:
            drop_dollars = data.loc[hug['end_index'], 'close'] - pivot_price
            days_diff = pivot_idx - hug['end_index']
            upper_hug_pullbacks.append({
                'hug_start_date': hug['start_date'],
                'hug_end_date': hug['end_date'],
                'hug_start_price': hug['start_price'],
                'hug_end_price': hug['end_price'],
                'intra_hug_change': hug['end_price'] - hug['start_price'],
                'trough_date': data.loc[pivot_idx, 'date'],
                'trough_price': pivot_price,
                'drop_dollars': -drop_dollars,
                'trading_days': days_diff
            })

    return {
        'lower_touch_bounces': lower_touch_bounces,
        'upper_touch_pullbacks': upper_touch_pullbacks,
        'lower_hug_bounces': lower_hug_bounces,
        'upper_hug_pullbacks': upper_hug_pullbacks,
    }
