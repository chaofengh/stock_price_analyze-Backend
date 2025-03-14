# tests/test_data_preparation.py

import pytest
import pandas as pd
from unittest.mock import patch
from analysis.data_preparation import prepare_stock_data, get_trading_period

@patch('analysis.data_preparation.fetch_stock_data')
def test_prepare_stock_data(mock_fetch):
    # Mock DataFrame to be returned by fetch_stock_data
    mock_data = pd.DataFrame({
        'date': pd.date_range(start='2020-01-01', periods=5),
        'close': [10, 11, 12, 11, 10]
    })
    # Return dictionary from the mock fetch function
    mock_fetch.return_value = {'TEST': mock_data}

    data_dict = prepare_stock_data(['TEST'])
    assert 'TEST' in data_dict
    df = data_dict['TEST']
    # Ensure Bollinger columns & RSI column are appended
    assert 'BB_upper' in df.columns
    assert 'BB_lower' in df.columns
    assert 'rsi' in df.columns

def test_get_trading_period():
    data = pd.DataFrame({
        'date': pd.date_range(start='2021-01-01', periods=3),
        'close': [100, 110, 105]
    })
    period_str, initial_price, final_price = get_trading_period(data)
    assert period_str == '2021-01-01 to 2021-01-03'
    assert initial_price == 100
    assert final_price == 105

    empty_data = pd.DataFrame(columns=['date', 'close'])
    period_str2, ip2, fp2 = get_trading_period(empty_data)
    assert period_str2 == 'No data'
    assert ip2 is None
    assert fp2 is None
