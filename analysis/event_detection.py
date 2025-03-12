#event_detection.py
import pandas as pd

# Touch detection remains largely the same
def detect_touches(data: pd.DataFrame) -> list:
    """
    Identifies single-day touches where the price touches the Bollinger Bands.
    """
    touches = []
    n = len(data)
    for i in range(n):
        row = data.loc[i]
        if row['high'] >= row['BB_upper']:
            touches.append({
                'date':  row['date'],
                'index': i,
                'band':  'upper',
                'price': float(row['close'])
            })
        if row['low'] <= row['BB_lower']:
            touches.append({
                'date':  row['date'],
                'index': i,
                'band':  'lower',
                'price': float(row['close'])
            })
    return touches

def detect_hug_events(data: pd.DataFrame, touches: list, threshold: float = 1.0):
    """
    Detects multi-day hugs where prices remain near the Bollinger Bands.
    """
    def near_band(row, band_key, threshold):
        if pd.isna(row[band_key]):
            return False
        diff_pct = abs(row['close'] - row[band_key]) / row[band_key] * 100
        return diff_pct <= threshold

    hug_events_upper = []
    hug_events_lower = []
    n = len(data)
    i = 0
    while i < n:
        up_touch = [t for t in touches if t['index'] == i and t['band'] == 'upper']
        if up_touch:
            day_indices = [i]
            j = i + 1
            while j < n and near_band(data.loc[j], 'BB_upper', threshold):
                day_indices.append(j)
                j += 1
            if len(day_indices) > 1:
                hug_events_upper.append({
                    'band': 'upper',
                    'start_index': day_indices[0],
                    'end_index': day_indices[-1],
                    'start_date': data.loc[day_indices[0], 'date'],
                    'end_date': data.loc[day_indices[-1], 'date'],
                    'start_price': float(data.loc[day_indices[0], 'close']),
                    'end_price': float(data.loc[day_indices[-1], 'close'])
                })
                i = day_indices[-1] + 1
                continue

        low_touch = [t for t in touches if t['index'] == i and t['band'] == 'lower']
        if low_touch:
            day_indices = [i]
            j = i + 1
            while j < n and near_band(data.loc[j], 'BB_lower', threshold):
                day_indices.append(j)
                j += 1
            if len(day_indices) > 1:
                hug_events_lower.append({
                    'band': 'lower',
                    'start_index': day_indices[0],
                    'end_index': day_indices[-1],
                    'start_date': data.loc[day_indices[0], 'date'],
                    'end_date': data.loc[day_indices[-1], 'date'],
                    'start_price': float(data.loc[day_indices[0], 'close']),
                    'end_price': float(data.loc[day_indices[-1], 'close'])
                })
                i = day_indices[-1] + 1
                continue
        i += 1

    return hug_events_upper, hug_events_lower

# New short-term high/low using a fixed lookahead window
def find_short_term_high(df, start_idx, window=5):
    """
    Finds the highest closing price in the next `window` trading days.
    """
    end_idx = min(len(df), start_idx + window)
    window_slice = df.iloc[start_idx:end_idx]
    if window_slice.empty:
        return (None, None)
    highest_idx = window_slice['close'].idxmax()
    highest_price = window_slice['close'].max()
    return (highest_idx, highest_price)

def find_short_term_low(df, start_idx, window=5):
    """
    Finds the lowest closing price in the next `window` trading days.
    """
    end_idx = min(len(df), start_idx + window)
    window_slice = df.iloc[start_idx:end_idx]
    if window_slice.empty:
        return (None, None)
    lowest_idx = window_slice['close'].idxmin()
    lowest_price = window_slice['close'].min()
    return (lowest_idx, lowest_price)


