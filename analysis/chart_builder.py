# chart_builder.py
import pandas as pd

def build_chart_data(data, touches) -> list:
    n = len(data)
    touch_indices = {t['index'] for t in touches}
    
    chart_data = []
    for i in range(n):
        chart_data.append({
            'date': data.loc[i, 'date'].strftime('%Y-%m-%d'),
            'close': float(data.loc[i, 'close']),
            'upper': float(data.loc[i, 'BB_upper']) if not pd.isna(data.loc[i, 'BB_upper']) else None,
            'lower': float(data.loc[i, 'BB_lower']) if not pd.isna(data.loc[i, 'BB_lower']) else None,
            'isTouch': (i in touch_indices),
        })
    return chart_data
