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

    # Verify volume metrics
    assert result['total_volume'] == 1000
    assert result['average_volume'] == 250

    # Verify price metrics
    assert result['max_price'] == 13
    assert result['min_price'] == 5
    assert result['percentage_change'] == pytest.approx(((8 - 6) / 6) * 100)

    # Verify volatility
    # Daily returns for 'close': [NaN, 0.83333, 0.09091, -0.33333]
    # Expected sample std ~ 0.59098
    assert result['volatility'] == pytest.approx(0.59098, rel=1e-2)

    # Verify average Bollinger Band width
    # BB_width = BB_upper - BB_lower: [5, 5, 8, 5] so mean is 5.75
    assert result['avg_BB_width'] == pytest.approx(5.75, rel=1e-2)

    # Verify RSI statistics
    # RSI_mean = (30 + 40 + 50 + 60) / 4 = 45
    assert result['RSI_mean'] == pytest.approx(45, rel=1e-2)
    # For RSI_std, the sample standard deviation for [30, 40, 50, 60]
    # Differences from the mean (45): [-15, -5, 5, 15] => squares: [225, 25, 25, 225]
    # Sum = 500, sample variance = 500 / 3 ≈ 166.67, std ≈ 12.9099
    assert result['RSI_std'] == pytest.approx(12.9099, rel=1e-2)

def test_compute_avg_hug_length():
    hug_list = [
        {'start_index': 0, 'end_index': 2},  # length = 3
        {'start_index': 4, 'end_index': 6},  # length = 3
    ]
    avg_length = compute_avg_hug_length(hug_list)
    assert avg_length == 3.0

    # Test empty list
    assert compute_avg_hug_length([]) == 0.0
