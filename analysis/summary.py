import numpy as np
import pandas as pd

from .data_fetcher import fetch_stock_data, fetch_stock_fundamentals
from .indicators import compute_bollinger_bands, compute_rsi
from .event_detection import (
    detect_touches, 
    detect_hug_events,
    find_short_term_high,
    find_short_term_low
)

def get_summary(symbol: str) -> dict:
    """
    Fetches stock data, computes indicators, detects events, and returns a summary.
    It computes bounce/pullback metrics for two lookahead windows (5 and 10 days)
    and aggregates touch and hug statistics for both the upper (resistance) and lower (support) Bollinger bands.
    """
    # -------------------------------
    # 1. FETCH AND PREPARE THE DATA
    # -------------------------------
    data = fetch_stock_data(symbol)
    data = compute_bollinger_bands(data)
    data = compute_rsi(data)

    n = len(data)
    start_date_str = data.loc[0, 'date'].strftime('%Y-%m-%d')
    end_date_str   = data.loc[n-1, 'date'].strftime('%Y-%m-%d')
    analysis_period = f"{start_date_str} to {end_date_str}"
    initial_price = float(data.loc[0, 'close'])
    final_price   = float(data.loc[n-1, 'close'])

    # -------------------------------
    # 2. DETECT TOUCHES & HUGS
    # -------------------------------
    touches = detect_touches(data)
    hug_events_upper, hug_events_lower = detect_hug_events(data, touches)

    # -------------------------------
    # 3. CALCULATE RAW BOUNCE/PULLBACK EVENTS
    #    (using a specified lookahead window)
    # -------------------------------
    def compute_bounces_and_pullbacks(window: int):
        """
        Returns raw bounce/pullback events for the specified lookahead window.
        """
        lower_touch_bounces = []
        upper_touch_pullbacks = []
        lower_hug_bounces = []
        upper_hug_pullbacks = []

        # --- Single-day touches -> measure bounce or pullback
        for t in touches:
            if t['band'] == 'lower':
                pivot_idx, pivot_price = find_short_term_high(data, t['index']+1, window=window)
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
                pivot_idx, pivot_price = find_short_term_low(data, t['index']+1, window=window)
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

        # --- Multi-day hugs -> measure bounce/pullback and record intra-hug change
        for hug in hug_events_lower:
            pivot_idx, pivot_price = find_short_term_high(data, hug['end_index']+1, window=window)
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
            pivot_idx, pivot_price = find_short_term_low(data, hug['end_index']+1, window=window)
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

    # Compute raw event arrays for two windows
    results_window_5 = compute_bounces_and_pullbacks(window=5)
    results_window_10 = compute_bounces_and_pullbacks(window=10)

    # -------------------------------
    # 4. CALCULATE AGGREGATED METRICS
    #    (per window)
    # -------------------------------
    def average(items, key):
        if not items:
            return None
        return sum(item[key] for item in items) / len(items)

    def compute_hug_length(hug_events):
        if not hug_events:
            return None
        lengths = [(h['end_index'] - h['start_index'] + 1) for h in hug_events]
        return np.mean(lengths)

    def computeAggregates(results, hug_events_upper, hug_events_lower):
        aggregates = {}
        # Upper Touch Metrics (Pullbacks)
        upper_touch_pullbacks = results.get('upper_touch_pullbacks', [])
        aggregates['avg_upper_touch_drop'] = average(upper_touch_pullbacks, 'drop_dollars')
        aggregates['avg_upper_touch_in_days'] = average(upper_touch_pullbacks, 'trading_days')

        # Lower Touch Metrics (Bounces)
        lower_touch_bounces = results.get('lower_touch_bounces', [])
        aggregates['avg_lower_touch_bounce'] = average(lower_touch_bounces, 'bounce_dollars')
        aggregates['avg_lower_touch_bounce_in_days'] = average(lower_touch_bounces, 'trading_days')

        # Upper Hug Metrics (Pullbacks)
        upper_hug_pullbacks = results.get('upper_hug_pullbacks', [])
        aggregates['avg_upper_hug_change'] = average(upper_hug_pullbacks, 'intra_hug_change')
        aggregates['avg_upper_hug_drop'] = average(upper_hug_pullbacks, 'drop_dollars')
        aggregates['avg_upper_hug_drop_in_days'] = average(upper_hug_pullbacks, 'trading_days')
        aggregates['avg_upper_hug_length_in_days'] = compute_hug_length(hug_events_upper)
        aggregates['avg_upper_hug_touch_count'] = len(hug_events_upper)

        # Lower Hug Metrics (Bounces)
        lower_hug_bounces = results.get('lower_hug_bounces', [])
        aggregates['avg_lower_hug_change'] = average(lower_hug_bounces, 'intra_hug_change')
        aggregates['avg_lower_hug_bounce'] = average(lower_hug_bounces, 'bounce_dollars')
        aggregates['avg_lower_hug_bounce_in_days'] = average(lower_hug_bounces, 'trading_days')
        aggregates['avg_lower_hug_length_in_days'] = compute_hug_length(hug_events_lower)
        aggregates['avg_lower_hug_touch_count'] = len(hug_events_lower)

        return aggregates

    aggregated_window_5 = computeAggregates(results_window_5, hug_events_upper, hug_events_lower)
    aggregated_window_10 = computeAggregates(results_window_10, hug_events_upper, hug_events_lower)

    # -------------------------------
    # 5. ADDITIONAL METRICS
    # -------------------------------
    total_volume = data['volume'].sum()
    average_volume = data['volume'].mean()
    max_price = data['high'].max()
    min_price = data['low'].min()
    percentage_change = (final_price - initial_price) / initial_price * 100

    # Volatility (std dev of daily returns)
    daily_returns = data['close'].pct_change()
    volatility = daily_returns.std()

    # Bollinger Band width stats
    data['BB_width'] = data['BB_upper'] - data['BB_lower']
    avg_BB_width = data['BB_width'].mean()

    # RSI stats
    rsi_mean = data['rsi'].mean()
    rsi_std  = data['rsi'].std()

    # Overall hug lengths (regardless of window)
    def avg_hug_length(hug_list):
        if not hug_list:
            return 0.0
        lengths = [(h['end_index'] - h['start_index'] + 1) for h in hug_list]
        return np.mean(lengths)

    avg_upper_hug_length = avg_hug_length(hug_events_upper)
    avg_lower_hug_length = avg_hug_length(hug_events_lower)

    # -------------------------------
    # 6. BUILD CHART DATA
    # -------------------------------
    touch_indices = {t['index'] for t in touches}
    hug_indices = set()
    for h in hug_events_lower + hug_events_upper:
        hug_indices.update(range(h['start_index'], h['end_index'] + 1))
    
    chart_data = []
    for i in range(n):
        chart_data.append({
            'date':   data.loc[i, 'date'].strftime('%Y-%m-%d'),
            'close':  float(data.loc[i, 'close']),
            'upper':  float(data.loc[i, 'BB_upper']) if not pd.isna(data.loc[i, 'BB_upper']) else None,
            'lower':  float(data.loc[i, 'BB_lower']) if not pd.isna(data.loc[i, 'BB_lower']) else None,
            'isTouch': (i in touch_indices),
            'isHug':   (i in hug_indices)
        })
    
    try:
        fundamentals = fetch_stock_fundamentals(symbol)
    except Exception as e:
        fundamentals = {}

    # -------------------------------
    # 7. COMPILE THE SUMMARY
    # -------------------------------
    summary = {
        'symbol': symbol,
        'analysis_period': analysis_period,
        'trading_days': n,
        'initial_price': initial_price,
        'final_price': final_price,
        'price_change_in_dollars': final_price - initial_price,
        'percentage_change': percentage_change,
        'total_volume': total_volume,
        'average_volume': average_volume,
        'max_price': max_price,
        'min_price': min_price,
        'volatility': volatility,
        'average_BB_width': avg_BB_width,
        'RSI_mean': rsi_mean,
        'RSI_std': rsi_std,

        'total_touches': len(touches),
        'upper_touches_count': len([t for t in touches if t['band'] == 'upper']),
        'lower_touches_count': len([t for t in touches if t['band'] == 'lower']),

        'hug_events_upper_count': len(hug_events_upper),
        'hug_events_lower_count': len(hug_events_lower),
        'avg_upper_hug_length': avg_upper_hug_length,
        'avg_lower_hug_length': avg_lower_hug_length,

        'chart_data': chart_data,

        # Raw bounce/pullback events for each window
        'window_5': results_window_5,
        'window_10': results_window_10,

        # Aggregated metrics for each window
        'aggregated_window_5': aggregated_window_5,
        'aggregated_window_10': aggregated_window_10,

        # Fundamental metrics
        'trailingPE': fundamentals.get("trailingPE"),
        'forwardPE': fundamentals.get("forwardPE"),
        'PEG': fundamentals.get("PEG"),
        'PGI': fundamentals.get("PGI"),
        'trailingPEG': fundamentals.get("trailingPEG"),
        'dividendYield': fundamentals.get("dividendYield"),
        'beta': fundamentals.get("beta"),
        'marketCap': fundamentals.get("marketCap"),
        'priceToBook': fundamentals.get("priceToBook"),
        'forwardEPS': fundamentals.get("forwardEPS"),
        'trailingEPS': fundamentals.get("trailingEPS"),
        'debtToEquity': fundamentals.get("debtToEquity"),
    }

    return summary
