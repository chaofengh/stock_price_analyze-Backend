# tests/test_additional_metrics.py

import pandas as pd
from analysis.additional_metrics import compute_additional_metrics, compute_avg_hug_length
import pytest

def test_compute_additional_metrics():
    data = pd.DataFrame({
        'volume': [100, 200, 300, 400],
        'high': [10, 12, 13, 9],
        'low': [5, 7, 8, 6],
        'close': [6, 11, 12, 8],
        'BB_upper': [10, 12, 14, 10],
        'BB_lower': [5, 7, 6, 5],
        'rsi': [30, 40, 50, 60],
    })

    initial_price = 6
    final_price = 8
    result = compute_additional_metrics(data, initial_price, final_price)

    assert result['total_volume'] == 1000
    assert result['average_volume'] == 250
    assert result['max_price'] == 13
    assert result['min_price'] == 5
    assert result['percentage_change'] == pytest.approx(((8 - 6) / 6) * 100)
    assert 'volatility' in result
    assert 'avg_BB_width' in result
    assert 'RSI_mean' in result
    assert 'RSI_std' in result

def test_compute_avg_hug_length():
    hug_list = [
        {'start_index': 0, 'end_index': 2},  # length = 3
        {'start_index': 4, 'end_index': 6},  # length = 3
    ]
    avg_length = compute_avg_hug_length(hug_list)
    assert avg_length == 3.0

    # Test empty list
    assert compute_avg_hug_length([]) == 0.0
