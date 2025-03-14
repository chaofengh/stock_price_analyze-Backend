# tests/test_event_detection.py

import pandas as pd
from analysis.event_detection import detect_touches, detect_hug_events, find_short_term_high, find_short_term_low

def test_detect_touches():
    df = pd.DataFrame({
        'date': pd.date_range('2021-01-01', periods=3),
        'high': [10, 11, 12],
        'low': [5, 5, 5],
        'close': [8, 11, 5],
        'BB_upper': [9, 10, 12],
        'BB_lower': [7, 6, 5],
    })
    touches = detect_touches(df)
    # We expect:
    # day 1: high(10) >= BB_upper(9)? => yes, so touches upper
    # day 2: high(11) >= BB_upper(10)? => yes, upper
    # day 3: low(5) <= BB_lower(5)? => yes, lower
    assert len(touches) == 3

def test_detect_hug_events():
    df = pd.DataFrame({
        'date': pd.date_range('2021-01-01', periods=5),
        'close': [10, 10.1, 10.2, 10.1, 10],
        'BB_upper': [10, 10, 10, 10, 10],
        'BB_lower': [5, 5, 5, 5, 5],
    })
    # All these days are near or above the upper band (close ~10 vs BB_upper=10)
    # We'll artificially trigger a "touch" on the first day
    touches = [{'date': df.loc[0, 'date'], 'index': 0, 'band': 'upper', 'price': 10.0}]
    ups, lows = detect_hug_events(df, touches, threshold=2.0)
    # We might expect to find one hug event in ups
    assert len(ups) == 1
    assert ups[0]['start_index'] == 0
    # No lower band touches
    assert len(lows) == 0

def test_find_short_term_high():
    df = pd.DataFrame({'close': [10, 12, 11, 13, 8, 15]})
    idx, price = find_short_term_high(df, start_idx=0, window=3)
    # Highest in next 3 days (index 0,1,2) => index=1 (close=12) or 3rd row is 11, so the max is 12
    assert idx == 1
    assert price == 12

def test_find_short_term_low():
    df = pd.DataFrame({'close': [10, 9, 8, 11, 7, 13]})
    idx, price = find_short_term_low(df, start_idx=1, window=3)
    # Next 3 days from index=1 => indices [1,2,3] => [9,8,11], the min is 8 at index=2
    assert idx == 2
    assert price == 8
