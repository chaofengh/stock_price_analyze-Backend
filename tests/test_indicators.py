# tests/test_indicators.py

import pandas as pd
import numpy as np
from analysis.indicators import compute_bollinger_bands, compute_rsi

def test_compute_bollinger_bands():
    df = pd.DataFrame({'close': [10, 11, 12, 13, 14, 15]})
    df = compute_bollinger_bands(df, timeperiod=3, nbdevup=1, nbdevdn=1)
    # Check that columns exist
    assert 'BB_upper' in df.columns
    assert 'BB_lower' in df.columns
    assert 'BB_middle' in df.columns
    # The actual numeric values can be tested with approximate checks if needed

def test_compute_rsi():
    df = pd.DataFrame({'close': [45, 46, 47, 50, 49, 48, 47, 45, 44, 43, 42]})
    df = compute_rsi(df, timeperiod=6)
    assert 'rsi' in df.columns
    # Just ensure no NaNs in the tail
    assert not df['rsi'].tail(1).isna().any()
