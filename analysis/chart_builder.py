# chart_builder.py
import pandas as pd

def build_chart_data(data, touches, hug_events) -> list:
    n = len(data)
    touch_indices = {t['index'] for t in touches}
    hug_indices = set()
    for h in hug_events:
        hug_indices.update(range(h['start_index'], h['end_index'] + 1))
    
    chart_data = []
    for i in range(n):
        chart_data.append({
            'date': data.loc[i, 'date'].strftime('%Y-%m-%d'),
            'close': float(data.loc[i, 'close']),
            'upper': float(data.loc[i, 'BB_upper']) if not pd.isna(data.loc[i, 'BB_upper']) else None,
            'lower': float(data.loc[i, 'BB_lower']) if not pd.isna(data.loc[i, 'BB_lower']) else None,
            'isTouch': (i in touch_indices),
            'isHug': (i in hug_indices),
            'volume': float(data.loc[i, 'volume'])
        })
    return chart_data
