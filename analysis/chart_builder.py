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
        row = data.iloc[i]
        upper = row.get('BB_upper')
        lower = row.get('BB_lower')
        chart_data.append({
            'date': row['date'].strftime('%Y-%m-%d'),
            'close': float(row['close']),
            'upper': float(upper) if upper is not None and not pd.isna(upper) else None,
            'lower': float(lower) if lower is not None and not pd.isna(lower) else None,
            'isTouch': (i in touch_indices),
        })
    return chart_data
