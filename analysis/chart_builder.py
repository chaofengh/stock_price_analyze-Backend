"""
chart_builder.py
Purpose: format chart-ready data for the frontend.
Pseudocode:
1) Build a set of touch indices.
2) For each row, emit OHLC + Bollinger + touch marker data.
"""
import pandas as pd


def _to_float_or_none(value):
    """Return a JSON-safe float or None for missing/non-numeric values."""
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_chart_data(data, touches) -> list:
    """Return a list of chart points with optional touch markers."""
    n = len(data)
    touch_indices = {t['index'] for t in touches}

    chart_data = []
    for i in range(n):
        row = data.iloc[i]
        chart_data.append({
            'date': row['date'].strftime('%Y-%m-%d'),
            'open': _to_float_or_none(row.get('open')),
            'high': _to_float_or_none(row.get('high')),
            'low': _to_float_or_none(row.get('low')),
            'close': _to_float_or_none(row.get('close')),
            'upper': _to_float_or_none(row.get('BB_upper')),
            'lower': _to_float_or_none(row.get('BB_lower')),
            'isTouch': (i in touch_indices),
        })
    return chart_data
