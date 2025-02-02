import os
import pandas as pd
import numpy as np
import talib
from alpha_vantage.timeseries import TimeSeries

def get_summary(symbol: str) -> dict:
    """
    Fetch daily data from Alpha Vantage, compute Bollinger Bands, RSI, 
    single-day touches, multi-day hugs, and compile a summary dictionary.
    Also includes 'isTouch'/'isHug' booleans in chart_data for each row.
    """

    # ------------------------------------------------------
    #  FETCH DATA FROM ALPHA VANTAGE & PREPARE THE DATAFRAME
    # ------------------------------------------------------
    alpha_vantage_api_key = os.environ.get("alpha_vantage_api_key")
    if not alpha_vantage_api_key:
        raise ValueError("Missing 'alpha_vantage_api_key' in environment")

    ts = TimeSeries(key=alpha_vantage_api_key, output_format='pandas')
    data, meta_data = ts.get_daily(symbol=symbol, outputsize='compact')
    
    data.sort_index(inplace=True)
    data.rename(columns={
        '1. open': 'open',
        '2. high': 'high',
        '3. low':  'low',
        '4. close': 'close',
        '5. volume': 'volume'
    }, inplace=True)
    
    # Compute Bollinger Bands
    upper, mid, lower = talib.BBANDS(
        data['close'].astype(float),
        timeperiod=20, nbdevup=2, nbdevdn=2, matype=0
    )
    data['BB_upper'] = upper
    data['BB_middle'] = mid
    data['BB_lower'] = lower
    
    data.reset_index(inplace=True)
    data.rename(columns={'index': 'date'}, inplace=True)
    data['date'] = pd.to_datetime(data['date'])
    
    # Compute RSI(6). Then define dynamic bounds from mean ± std
    data['rsi'] = talib.RSI(data['close'], timeperiod=6)
    rsi_mean = data['rsi'].mean()
    rsi_std  = data['rsi'].std()
    rsi_upper_bound = rsi_mean + rsi_std
    rsi_lower_bound = rsi_mean - rsi_std
    
    n = len(data)

    # ----------------------------------
    # 1) IDENTIFY SINGLE-DAY "TOUCHES"
    # ----------------------------------
    touches = []
    for i in range(n):
        row = data.loc[i]
        # Touch the upper band if today's high >= BB_upper
        if row['high'] >= row['BB_upper']:
            touches.append({
                'date':  row['date'],
                'index': i,
                'band':  'upper',
                'price': float(row['close'])
            })
        # Touch the lower band if today's low <= BB_lower
        if row['low'] <= row['BB_lower']:
            touches.append({
                'date':  row['date'],
                'index': i,
                'band':  'lower',
                'price': float(row['close'])
            })

    # ----------------------------------
    # 2) IDENTIFY MULTI-DAY "HUGS"
    # ----------------------------------
    HUG_THRESHOLD = 1.0  # e.g. 1% difference from band
    
    def near_upper_band(row, threshold=HUG_THRESHOLD):
        if pd.isna(row['BB_upper']):
            return False
        diff_pct = abs(row['close'] - row['BB_upper']) / row['BB_upper'] * 100
        return (diff_pct <= threshold)
    
    def near_lower_band(row, threshold=HUG_THRESHOLD):
        if pd.isna(row['BB_lower']):
            return False
        diff_pct = abs(row['close'] - row['BB_lower']) / row['BB_lower'] * 100
        return (diff_pct <= threshold)
    
    hug_events_upper = []
    hug_events_lower = []
    
    i = 0
    while i < n:
        up_touch = [t for t in touches if t['index'] == i and t['band'] == 'upper']
        if up_touch:
            day_indices = [i]
            j = i + 1
            while j < n:
                if near_upper_band(data.loc[j]):
                    day_indices.append(j)
                    j += 1
                else:
                    break
            if len(day_indices) > 1:
                start_idx = day_indices[0]
                end_idx   = day_indices[-1]
                hug_events_upper.append({
                    'band': 'upper',
                    'start_index': start_idx,
                    'end_index':   end_idx,
                    'start_date':  data.loc[start_idx, 'date'],
                    'end_date':    data.loc[end_idx,   'date'],
                    'start_price': float(data.loc[start_idx, 'close']),
                    'end_price':   float(data.loc[end_idx,   'close'])
                })
                i = end_idx + 1
                continue
    
        low_touch = [t for t in touches if t['index'] == i and t['band'] == 'lower']
        if low_touch:
            day_indices = [i]
            j = i + 1
            while j < n:
                if near_lower_band(data.loc[j]):
                    day_indices.append(j)
                    j += 1
                else:
                    break
            if len(day_indices) > 1:
                start_idx = day_indices[0]
                end_idx   = day_indices[-1]
                hug_events_lower.append({
                    'band': 'lower',
                    'start_index': start_idx,
                    'end_index':   end_idx,
                    'start_date':  data.loc[start_idx, 'date'],
                    'end_date':    data.loc[end_idx,   'date'],
                    'start_price': float(data.loc[start_idx, 'close']),
                    'end_price':   float(data.loc[end_idx,   'close'])
                })
                i = end_idx + 1
                continue
        i += 1
    
    # ----------------------------------
    # 3) HELPERS FOR RSI-BASED PIVOTS
    # ----------------------------------
    def find_short_term_high_rsi(df, start_idx, rsi_upper):
        """
        Looks for a short-term 'peak' after RSI crosses above rsi_upper.
        """
        n_ = len(df)
        phase1_found = False
        phase2_peak  = -999
        phase2_idx   = None
        
        i_ = start_idx
        while i_ < n_:
            rsi_val   = df.loc[i_, 'rsi']
            close_val = df.loc[i_, 'close']
            if not phase1_found:
                if rsi_val > rsi_upper:
                    phase1_found = True
                    phase2_peak  = close_val
                    phase2_idx   = i_
            else:
                if rsi_val > rsi_upper:
                    # Possibly even higher peak
                    if close_val > phase2_peak:
                        phase2_peak = close_val
                        phase2_idx  = i_
                else:
                    # The moment RSI falls below rsi_upper => we stop
                    return (phase2_idx, phase2_peak)
            i_ += 1
        
        if phase1_found and phase2_idx is not None:
            return (phase2_idx, phase2_peak)
        return (None, None)
    
    def find_short_term_low_rsi(df, start_idx, rsi_lower):
        """
        Looks for a short-term 'trough' after RSI crosses below rsi_lower.
        """
        n_ = len(df)
        phase1_found   = False
        phase2_trough  = 999999
        phase2_idx     = None
        
        i_ = start_idx
        while i_ < n_:
            rsi_val   = df.loc[i_, 'rsi']
            close_val = df.loc[i_, 'close']
            if not phase1_found:
                if rsi_val < rsi_lower:
                    phase1_found  = True
                    phase2_trough = close_val
                    phase2_idx    = i_
            else:
                if rsi_val < rsi_lower:
                    # Possibly even lower trough
                    if close_val < phase2_trough:
                        phase2_trough = close_val
                        phase2_idx    = i_
                else:
                    # The moment RSI climbs above rsi_lower => we stop
                    return (phase2_idx, phase2_trough)
            i_ += 1
        
        if phase1_found and phase2_idx is not None:
            return (phase2_idx, phase2_trough)
        return (None, None)
    
    # ----------------------------------
    # 4) BUILD SUMMARY DICT W/ METRICS
    # ----------------------------------
    start_date_str   = data.loc[0, 'date'].strftime('%Y-%m-%d')
    end_date_str     = data.loc[n-1, 'date'].strftime('%Y-%m-%d')
    analysis_period  = f"{start_date_str} to {end_date_str}"
    initial_price    = float(data.loc[0, 'close'])
    final_price      = float(data.loc[n-1, 'close'])

    all_upper = [t for t in touches if t['band'] == 'upper']
    all_lower = [t for t in touches if t['band'] == 'lower']
    
    summary = {
        'symbol': symbol,
        'analysis_period': analysis_period,
        'trading_days': n,
        'initial_price': initial_price,
        'final_price': final_price,
        'price_change_in_dollars': final_price - initial_price,
        
        'total_touches': len(touches),
        'upper_touches_count': len(all_upper),
        'lower_touches_count': len(all_lower),
        
        'hug_events_upper_count': len(hug_events_upper),
        'hug_events_lower_count': len(hug_events_lower),
        
        # AVERAGE LENGTHS, BOUNCES, ETC. — placeholders to be filled
        'avg_upper_hug_length': 0.0,
        'avg_lower_hug_length': 0.0,
        'avg_upper_hug_touch_count': 0.0,  # can treat as same as length
        'avg_lower_hug_touch_count': 0.0,
        
        'avg_lower_touch_bounce': 0.0,
        'avg_lower_touch_bounce_in_days': 0.0,
        'avg_upper_touch_drop': 0.0,
        'avg_upper_touch_in_days': 0.0,
        
        'avg_lower_hug_bounce': 0.0,
        'avg_lower_hug_bounce_in_days': 0.0,
        'avg_upper_hug_drop': 0.0,
        'avg_upper_hug_drop_in_days': 0.0,
        
        'avg_upper_hug_change': 0.0,
        'avg_lower_hug_change': 0.0,
        
        # Lists of bounce/pullback events
        'lower_touch_bounces': [],
        'upper_touch_pullbacks': [],
        'lower_hug_bounces': [],
        'upper_hug_pullbacks': [],
        
        'hug_events_lower': hug_events_lower,
        'hug_events_upper': hug_events_upper,
    }
    
    # ---------- Single-day touches => measure bounce or pullback ----------
    for t in touches:
        if t['band'] == 'lower':
            # Look for short-term high after the touch
            pivot_idx, pivot_close = find_short_term_high_rsi(data, t['index']+1, rsi_upper_bound)
            if pivot_idx is not None:
                bounce_dollars = pivot_close - data.loc[t['index'], 'close']
                days_diff = pivot_idx - t['index']
                summary['lower_touch_bounces'].append({
                    'touch_date':  t['date'],
                    'touch_price': t['price'],
                    'peak_date':   data.loc[pivot_idx, 'date'],
                    'peak_price':  pivot_close,
                    'bounce_dollars': bounce_dollars,
                    'trading_days':   days_diff
                })
        else:
            # Look for short-term low after upper touch
            pivot_idx, pivot_close = find_short_term_low_rsi(data, t['index']+1, rsi_lower_bound)
            if pivot_idx is not None:
                drop_dollars = data.loc[t['index'], 'close'] - pivot_close
                days_diff    = pivot_idx - t['index']
                summary['upper_touch_pullbacks'].append({
                    'touch_date':   t['date'],
                    'touch_price':  t['price'],
                    'trough_date':  data.loc[pivot_idx, 'date'],
                    'trough_price': pivot_close,
                    'drop_dollars': drop_dollars,
                    'trading_days': days_diff
                })
    
    # ---------- Multi-day hugs => measure bounce/pullback afterward ----------
    def find_avg_price_change_in_hugs(hug_list):
        if not hug_list:
            return 0.0
        changes = [hug['end_price'] - hug['start_price'] for hug in hug_list]
        return np.mean(changes)
    
    # For lower hugs
    for hug in hug_events_lower:
        pivot_idx, pivot_close = find_short_term_high_rsi(data, hug['end_index']+1, rsi_upper_bound)
        if pivot_idx is not None:
            bounce_dollars = pivot_close - data.loc[hug['end_index'], 'close']
            days_diff      = pivot_idx - hug['end_index']
            summary['lower_hug_bounces'].append({
                'hug_start_date': hug['start_date'],
                'hug_end_date':   hug['end_date'],
                'hug_start_price': hug['start_price'],
                'hug_end_price':   hug['end_price'],
                'peak_date':       data.loc[pivot_idx, 'date'],
                'peak_price':      pivot_close,
                'bounce_dollars':  bounce_dollars,
                'trading_days':    days_diff
            })

    # For upper hugs
    for hug in hug_events_upper:
        pivot_idx, pivot_close = find_short_term_low_rsi(data, hug['end_index']+1, rsi_lower_bound)
        if pivot_idx is not None:
            drop_dollars = data.loc[hug['end_index'], 'close'] - pivot_close
            days_diff    = pivot_idx - hug['end_index']
            summary['upper_hug_pullbacks'].append({
                'hug_start_date':  hug['start_date'],
                'hug_end_date':    hug['end_date'],
                'hug_start_price': hug['start_price'],
                'hug_end_price':   hug['end_price'],
                'trough_date':     data.loc[pivot_idx, 'date'],
                'trough_price':    pivot_close,
                'drop_dollars':    drop_dollars,
                'trading_days':    days_diff
            })
    
    # Compute average hug lengths, changes, etc.
    def avg_hug_length(hug_list):
        if not hug_list:
            return 0.0
        lengths = [(h['end_index'] - h['start_index'] + 1) for h in hug_list]
        return np.mean(lengths)

    summary['avg_upper_hug_length'] = avg_hug_length(hug_events_upper)
    summary['avg_lower_hug_length'] = avg_hug_length(hug_events_lower)
    summary['avg_upper_hug_touch_count'] = summary['avg_upper_hug_length']
    summary['avg_lower_hug_touch_count'] = summary['avg_lower_hug_length']

    def mean_of(key, arr):
        if not arr:
            return 0.0
        return np.mean([x[key] for x in arr])
    
    def mean_days(arr):
        if not arr:
            return 0.0
        return np.mean([x['trading_days'] for x in arr])

    summary['avg_lower_touch_bounce']          = mean_of('bounce_dollars', summary['lower_touch_bounces'])
    summary['avg_lower_touch_bounce_in_days']  = mean_days(summary['lower_touch_bounces'])
    summary['avg_upper_touch_drop']            = mean_of('drop_dollars',   summary['upper_touch_pullbacks'])
    summary['avg_upper_touch_in_days']         = mean_days(summary['upper_touch_pullbacks'])
    summary['avg_lower_hug_bounce']            = mean_of('bounce_dollars', summary['lower_hug_bounces'])
    summary['avg_lower_hug_bounce_in_days']    = mean_days(summary['lower_hug_bounces'])
    summary['avg_upper_hug_drop']              = mean_of('drop_dollars',   summary['upper_hug_pullbacks'])
    summary['avg_upper_hug_drop_in_days']      = mean_days(summary['upper_hug_pullbacks'])
    
    summary['avg_upper_hug_change'] = find_avg_price_change_in_hugs(hug_events_upper)
    summary['avg_lower_hug_change'] = find_avg_price_change_in_hugs(hug_events_lower)

    # ----------------------------------
    # 5) BUILD chart_data w/ isTouch/isHug
    # ----------------------------------
    touch_indices = set(t['index'] for t in touches)
    
    hug_indices = set()
    for h in hug_events_lower:
        hug_indices.update(range(h['start_index'], h['end_index']+1))
    for h in hug_events_upper:
        hug_indices.update(range(h['start_index'], h['end_index']+1))

    chart_data = []
    for i in range(n):
        chart_data.append({
            'date':   data.loc[i, 'date'].strftime('%Y-%m-%d'),
            'close':  float(data.loc[i, 'close']),
            'upper':  float(data.loc[i, 'BB_upper']) if not pd.isna(data.loc[i, 'BB_upper']) else None,
            'lower':  float(data.loc[i, 'BB_lower']) if not pd.isna(data.loc[i, 'BB_lower']) else None,
            'isTouch': (i in touch_indices),
            'isHug':   (i in hug_indices)
        })
    summary['chart_data'] = chart_data

    return summary
