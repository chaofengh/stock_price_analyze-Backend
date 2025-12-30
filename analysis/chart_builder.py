"""
chart_builder.py
Purpose: format chart-ready data for the frontend.
Pseudocode:
1) Build a set of touch indices.
2) For each row, emit a minimal dict with date/close/bands.
"""
import pandas as pd

def build_chart_data(data, touches) -> list:
    """Return a list of chart points with optional touch markers."""
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
