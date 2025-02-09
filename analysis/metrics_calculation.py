# metrics_calculation.py
import numpy as np

def average(items, key):
    if not items:
        return None
    return sum(item[key] for item in items) / len(items)

def compute_hug_length(hug_events):
    if not hug_events:
        return None
    lengths = [(h['end_index'] - h['start_index'] + 1) for h in hug_events]
    return np.mean(lengths)

def compute_aggregates(results, hug_events_upper, hug_events_lower) -> dict:
    aggregates = {}
    upper_touch_pullbacks = results.get('upper_touch_pullbacks', [])
    aggregates['avg_upper_touch_drop'] = average(upper_touch_pullbacks, 'drop_dollars')
    aggregates['avg_upper_touch_in_days'] = average(upper_touch_pullbacks, 'trading_days')

    lower_touch_bounces = results.get('lower_touch_bounces', [])
    aggregates['avg_lower_touch_bounce'] = average(lower_touch_bounces, 'bounce_dollars')
    aggregates['avg_lower_touch_bounce_in_days'] = average(lower_touch_bounces, 'trading_days')

    upper_hug_pullbacks = results.get('upper_hug_pullbacks', [])
    aggregates['avg_upper_hug_change'] = average(upper_hug_pullbacks, 'intra_hug_change')
    aggregates['avg_upper_hug_drop'] = average(upper_hug_pullbacks, 'drop_dollars')
    aggregates['avg_upper_hug_drop_in_days'] = average(upper_hug_pullbacks, 'trading_days')
    aggregates['avg_upper_hug_length_in_days'] = compute_hug_length(hug_events_upper)
    aggregates['avg_upper_hug_touch_count'] = len(hug_events_upper)

    lower_hug_bounces = results.get('lower_hug_bounces', [])
    aggregates['avg_lower_hug_change'] = average(lower_hug_bounces, 'intra_hug_change')
    aggregates['avg_lower_hug_bounce'] = average(lower_hug_bounces, 'bounce_dollars')
    aggregates['avg_lower_hug_bounce_in_days'] = average(lower_hug_bounces, 'trading_days')
    aggregates['avg_lower_hug_length_in_days'] = compute_hug_length(hug_events_lower)
    aggregates['avg_lower_hug_touch_count'] = len(hug_events_lower)

    return aggregates
