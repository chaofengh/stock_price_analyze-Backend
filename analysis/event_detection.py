#event_detection.py
import pandas as pd

# Touch detection remains largely the same
def process_bollinger_touches(data, mode='alert'):
    """
    Process Bollinger Band touches in two modes.
    
    For mode 'alert':
      - Expects data as a dict {symbol: DataFrame}.
      - Checks the latest row for each symbol.
      - Returns a list of alert dictionaries including:
            "symbol", "close_price", "bb_upper", "bb_lower", "touched_side", "recent_closes".
    
    For mode 'historical':
      - Expects data as a DataFrame for a single ticker.
      - Iterates over all rows and returns a list of touch events with:
            "date", "index", "band" (either 'upper' or 'lower'), and "price".
    """
    results = []
    
    if mode == 'alert':
        # data is a dict of {symbol: DataFrame}
        for symbol, df in data.items():
            if df.empty:
                continue
            last_row = df.iloc[-1]
            required_fields = ['close', 'BB_upper', 'BB_lower', 'high', 'low']
            if any(last_row[field] is None for field in required_fields):
                continue

            touched_side = None
            if last_row['high'] >= last_row['BB_upper']:
                touched_side = "Upper"
            elif last_row['low'] <= last_row['BB_lower']:
                touched_side = "Lower"
            
            if touched_side:
                results.append({
                    "symbol": symbol,
                    "close_price": float(last_row['close']),
                    "bb_upper": float(last_row['BB_upper']),
                    "bb_lower": float(last_row['BB_lower']),
                    'low_price': float(last_row['low']),
                    'high_price': float(last_row['high']),
                    "touched_side": touched_side,
                    "recent_closes": df['close'].tail(30).tolist()
                })
                
    elif mode == 'historical':
        # data is a DataFrame for a single ticker
        n = len(data)
        for i in range(n):
            row = data.iloc[i]
            required_fields = ['date', 'close', 'BB_upper', 'BB_lower', 'high', 'low']
            if any(row[field] is None for field in required_fields):
                continue
            if row['high'] >= row['BB_upper']:
                results.append({
                    "date": row['date'],
                    "index": i,
                    "band": "upper",
                    "price": float(row['close'])
                })
            if row['low'] <= row['BB_lower']:
                results.append({
                    "date": row['date'],
                    "index": i,
                    "band": "lower",
                    "price": float(row['close'])
                })
    return results


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


