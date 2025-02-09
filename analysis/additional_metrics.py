# additional_metrics.py
import numpy as np
import pandas as pd

def compute_additional_metrics(data, initial_price, final_price) -> dict:
    total_volume = data['volume'].sum()
    average_volume = data['volume'].mean()
    max_price = data['high'].max()
    min_price = data['low'].min()
    percentage_change = (final_price - initial_price) / initial_price * 100

    daily_returns = data['close'].pct_change()
    volatility = daily_returns.std()

    data['BB_width'] = data['BB_upper'] - data['BB_lower']
    avg_BB_width = data['BB_width'].mean()

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
    if not hug_list:
        return 0.0
    lengths = [(h['end_index'] - h['start_index'] + 1) for h in hug_list]
    return np.mean(lengths)
