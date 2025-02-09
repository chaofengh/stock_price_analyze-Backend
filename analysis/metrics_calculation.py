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
    
    # Upper Touch Pullbacks
    upper_touch_pullbacks = results.get('upper_touch_pullbacks', [])
    aggregates['avg_upper_touch_drop'] = average(upper_touch_pullbacks, 'drop_dollars')
    aggregates['avg_upper_touch_in_days'] = average(upper_touch_pullbacks, 'trading_days')
    aggregates['upper_touch_count']= len(upper_touch_pullbacks)
    # Compute upper_touch_accuracy: ratio of negative drop_dollars to total
    if upper_touch_pullbacks:
        count_negative = sum(1 for item in upper_touch_pullbacks if item['drop_dollars'] < 0)
        aggregates['upper_touch_accuracy'] = count_negative / len(upper_touch_pullbacks)
    else:
        aggregates['upper_touch_accuracy'] = None

    # Lower Touch Bounces
    lower_touch_bounces = results.get('lower_touch_bounces', [])
    aggregates['avg_lower_touch_bounce'] = average(lower_touch_bounces, 'bounce_dollars')
    aggregates['avg_lower_touch_bounce_in_days'] = average(lower_touch_bounces, 'trading_days')
    aggregates['lower_touch_count'] = len(lower_touch_bounces)
    # Compute lower_touch_accuracy: ratio of positive bounce_dollars to total
    if lower_touch_bounces:
        count_positive = sum(1 for item in lower_touch_bounces if item['bounce_dollars'] > 0)
        aggregates['lower_touch_accuracy'] = count_positive / len(lower_touch_bounces)
    else:
        aggregates['lower_touch_accuracy'] = None

    # Upper Hug Pullbacks
    upper_hug_pullbacks = results.get('upper_hug_pullbacks', [])
    aggregates['avg_upper_hug_change'] = average(upper_hug_pullbacks, 'intra_hug_change')
    aggregates['avg_upper_hug_drop'] = average(upper_hug_pullbacks, 'drop_dollars')
    aggregates['avg_upper_hug_drop_in_days'] = average(upper_hug_pullbacks, 'trading_days')
    aggregates['avg_upper_hug_length_in_days'] = compute_hug_length(hug_events_upper)
    # Compute upper_hug_accuracy: ratio of negative drop_dollars to total
    if upper_hug_pullbacks:
        count_negative_hug = sum(1 for item in upper_hug_pullbacks if item['drop_dollars'] < 0)
        aggregates['upper_hug_accuracy'] = count_negative_hug / len(upper_hug_pullbacks)
    else:
        aggregates['upper_hug_accuracy'] = None

    # Lower Hug Bounces
    lower_hug_bounces = results.get('lower_hug_bounces', [])
    aggregates['avg_lower_hug_change'] = average(lower_hug_bounces, 'intra_hug_change')
    aggregates['avg_lower_hug_bounce'] = average(lower_hug_bounces, 'bounce_dollars')
    aggregates['avg_lower_hug_bounce_in_days'] = average(lower_hug_bounces, 'trading_days')
    aggregates['avg_lower_hug_length_in_days'] = compute_hug_length(hug_events_lower)
    aggregates['avg_lower_hug_touch_count'] = len(hug_events_lower)
    # Compute lower_hug_accuracy: ratio of positive bounce_dollars to total
    if lower_hug_bounces:
        count_positive_hug = sum(1 for item in lower_hug_bounces if item['bounce_dollars'] > 0)
        aggregates['lower_hug_accuracy'] = count_positive_hug / len(lower_hug_bounces)
    else:
        aggregates['lower_hug_accuracy'] = None

    return aggregates
