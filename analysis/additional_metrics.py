"""
additional_metrics.py
Purpose: compute extra summary stats for a price series.
Pseudocode:
1) Compute volume and price ranges.
2) Compute volatility and band width averages.
3) Compute RSI mean and std.
"""
import numpy as np

def compute_additional_metrics(data, initial_price, final_price) -> dict:
    """Compute helper metrics from a single price DataFrame."""
    total_volume = data['volume'].sum()
    average_volume = data['volume'].mean()
    max_price = data['high'].max()
    min_price = data['low'].min()
    percentage_change = (final_price - initial_price) / initial_price * 100

    daily_returns = data['close'].pct_change()
    volatility = daily_returns.std()

    bb_width = data['BB_upper'] - data['BB_lower']
    avg_BB_width = bb_width.mean()

    rsi_mean = data['rsi'].mean()
    rsi_std  = data['rsi'].std()

    return {
        'total_volume': total_volume,
        'average_volume': average_volume,
        'max_price': max_price,
        'min_price': min_price,
        'percentage_change': percentage_change,
        'volatility': volatility,
        'avg_BB_width': avg_BB_width,
        'RSI_mean': rsi_mean,
        'RSI_std': rsi_std,
    }

def compute_avg_hug_length(hug_list) -> float:
    """Return average hug length in days (0.0 if empty)."""
    if not hug_list:
        return 0.0
    lengths = [(h['end_index'] - h['start_index'] + 1) for h in hug_list]
    return np.mean(lengths)
