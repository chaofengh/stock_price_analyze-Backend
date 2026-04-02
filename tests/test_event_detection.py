# tests/test_event_detection.py

import numpy as np
import pandas as pd
from analysis.event_detection import process_bollinger_touches, detect_hug_events, find_short_term_high, find_short_term_low

def test_detect_touches():
    df = pd.DataFrame({
        'date': pd.date_range('2021-01-01', periods=3),
        'high': [10, 11, 12],
        'low': [5, 5, 5],
        'close': [8, 11, 5],
        'BB_upper': [9, 10, 12],
        'BB_lower': [7, 6, 5],
    })
    touches = process_bollinger_touches(df,mode='historical')
    # We expect:
    # day 1: high(10) >= BB_upper(9)? => yes, so touches upper
    # day 1: low(5) <= BB_lower(7)? => yes, so touches lower
    # day 2: high(11) >= BB_upper(10)? => yes, upper
    # day 2: low(5) <= BB_lower(6)? => yes, lower
    # day 3: low(5) <= BB_lower(5)? => yes, lower
    # day 3: high(12) >= BB_upper(12)? => yes, upper
    assert len(touches) == 6

def test_detect_hug_events():
    # build a 5-day DataFrame with flat Bollinger bands
    df = pd.DataFrame({
        'date': pd.date_range('2021-01-01', periods=5),
        'close': [10, 10.1, 10.2, 10.1, 10],
        'BB_upper': [10, 10, 10, 10, 10],
        'BB_lower': [5, 5, 5, 5, 5],
    })

    # artificially trigger touches on days 0 and 1 (consecutive)
    touches = [
        {'date': df.loc[0, 'date'], 'index': 0, 'band': 'upper', 'price': 10.0},
        {'date': df.loc[1, 'date'], 'index': 1, 'band': 'upper', 'price': 10.1},
    ]

    # require at least 2 consecutive touches to form a hug event
    ups, lows = detect_hug_events(df, touches, min_group_len=2)

    # we should get exactly one "upper" hug event covering indices 0→1
    assert len(ups) == 1

    event = ups[0]
    assert event['band']        == 'upper'
    assert event['start_index'] == 0
    assert event['end_index']   == 1
    assert event['start_date']  == df.loc[0, 'date']
    assert event['end_date']    == df.loc[1, 'date']
    assert event['start_price'] == 10.0
    assert event['end_price']   == 10.1

    # no lower‐band hugs
    assert lows == []


def test_process_bollinger_touches_assigns_session_index_for_valid_rows_only():
    df = pd.DataFrame({
        "date": pd.to_datetime(["2024-07-03", "2024-07-05", "2024-07-08"]),
        "high": [11.0, np.nan, 12.0],
        "low": [6.0, np.nan, 6.0],
        "close": [10.0, np.nan, 11.0],
        "BB_upper": [10.0, np.nan, 10.0],
        "BB_lower": [5.0, np.nan, 5.0],
    })

    touches = process_bollinger_touches(df, mode="historical")
    upper_touches = [touch for touch in touches if touch["band"] == "upper"]

    assert [touch["index"] for touch in upper_touches] == [0, 2]
    assert [touch["session_index"] for touch in upper_touches] == [0, 1]


def test_detect_hug_events_holiday_gap_is_single_consecutive_streak():
    df = pd.DataFrame({
        "date": pd.to_datetime(["2024-07-04", "2024-07-05", "2024-07-08"]),
        "high": [11.0, np.nan, 12.0],
        "low": [6.0, np.nan, 6.0],
        "close": [10.0, np.nan, 11.0],
        "BB_upper": [10.0, np.nan, 10.0],
        "BB_lower": [5.0, np.nan, 5.0],
    })

    touches = process_bollinger_touches(df, mode="historical")
    upper_events, _ = detect_hug_events(df, touches, min_group_len=2)

    assert len(upper_events) == 1
    event = upper_events[0]
    assert event["start_index"] == 0
    assert event["end_index"] == 2
    assert event["start_session_index"] == 0
    assert event["end_session_index"] == 1
    assert event["touch_count"] == 2


def test_detect_hug_events_multi_day_market_closure_is_single_streak():
    # Thanksgiving-style closure gap between touches:
    # Wednesday touch, then two non-trading/invalid rows, then Monday touch.
    df = pd.DataFrame({
        "date": pd.to_datetime(["2024-11-27", "2024-11-28", "2024-11-29", "2024-12-02"]),
        "high": [11.0, np.nan, np.nan, 12.0],
        "low": [6.0, np.nan, np.nan, 6.0],
        "close": [10.0, np.nan, np.nan, 11.0],
        "BB_upper": [10.0, np.nan, np.nan, 10.0],
        "BB_lower": [5.0, np.nan, np.nan, 5.0],
    })

    touches = process_bollinger_touches(df, mode="historical")
    upper_events, _ = detect_hug_events(df, touches, min_group_len=2)

    assert len(upper_events) == 1
    event = upper_events[0]
    assert event["start_index"] == 0
    assert event["end_index"] == 3
    assert event["start_session_index"] == 0
    assert event["end_session_index"] == 1
    assert event["touch_count"] == 2

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
