import pandas as pd

from .data_fetcher_utils import _month_to_quarter, _normalize_line_name


def safe_float(value):
    try:
        if value is None:
            return None
        if isinstance(value, str):
            cleaned = value.strip()
            if not cleaned or cleaned.lower() in ("none", "nan", "na", "n/a", "null"):
                return None
            value = cleaned.replace(",", "")
        value = float(value)
    except Exception:
        return None
    if value != value or value in (float("inf"), float("-inf")):
        return None
    return value


def safe_div(numerator, denominator):
    if numerator is None or denominator is None:
        return None
    if denominator == 0:
        return None
    return numerator / denominator


def statement_columns_by_recency(df):
    if df is None or getattr(df, "empty", True):
        return []
    cols = list(getattr(df, "columns", []))
    if not cols:
        return []
    parsed = []
    for col in cols:
        try:
            parsed_col = pd.to_datetime(col)
        except Exception:
            parsed_col = pd.NaT
        parsed.append((parsed_col, col))
    parsed_valid = [item for item in parsed if pd.notna(item[0])]
    if parsed_valid:
        parsed_valid.sort(key=lambda item: item[0], reverse=True)
        return [item[1] for item in parsed_valid]
    return cols


def latest_statement_column(df):
    cols = statement_columns_by_recency(df)
    return cols[0] if cols else None


def statement_float(df, keys):
    if df is None or getattr(df, "empty", True):
        return None
    latest_col = latest_statement_column(df)
    if latest_col is None:
        return None
    target_keys = {_normalize_line_name(key) for key in keys}
    for idx in df.index:
        normalized = _normalize_line_name(idx)
        if normalized in target_keys:
            value = df.loc[idx, latest_col]
            if isinstance(value, pd.Series):
                value = value.iloc[0]
            return safe_float(value)
    return None


def statement_value_at(df, keys, col):
    if df is None or getattr(df, "empty", True):
        return None
    if col is None or col not in getattr(df, "columns", []):
        return None
    target_keys = {_normalize_line_name(key) for key in keys}
    for idx in df.index:
        normalized = _normalize_line_name(idx)
        if normalized in target_keys:
            value = df.loc[idx, col]
            if isinstance(value, pd.Series):
                value = value.iloc[0]
            return safe_float(value)
    return None


def av_value(report, keys):
    if not isinstance(report, dict):
        return None
    for key in keys:
        if key in report:
            return safe_float(report.get(key))
    return None


def alpha_report_map(reports):
    if not isinstance(reports, list):
        return {}
    mapped = {}
    for report in reports:
        if not isinstance(report, dict):
            continue
        date = report.get("fiscalDateEnding")
        if date:
            mapped[date] = report
    return mapped


def alpha_period_key(date_str, annual=False):
    if not date_str or len(date_str) < 7:
        return None
    year = date_str[:4]
    if annual:
        return (year, "annual")
    quarter = _month_to_quarter(date_str[5:7])
    if quarter is None:
        return None
    return (year, quarter)


def alpha_report_map_by_period(reports, annual=False):
    if not isinstance(reports, list):
        return {}
    mapped = {}
    for report in reports:
        if not isinstance(report, dict):
            continue
        key = alpha_period_key(report.get("fiscalDateEnding"), annual=annual)
        if key:
            mapped[key] = report
    return mapped


def alpha_report_lookup(period_map, date_map, date_str, annual=False):
    key = alpha_period_key(date_str, annual=annual)
    if key and key in period_map:
        return period_map.get(key) or {}
    if date_str and date_map:
        return date_map.get(date_str) or {}
    return {}


def alpha_dates(reports):
    if not isinstance(reports, list):
        return []
    dates = [r.get("fiscalDateEnding") for r in reports if isinstance(r, dict)]
    return sorted([d for d in dates if d], reverse=True)


def split_trends(series):
    if not series:
        return None
    recent = list(reversed(series[:4]))
    prior = list(reversed(series[4:8]))
    return {"recent": recent, "prior": prior}


def latest_series_value(series):
    for value in series or []:
        if value is not None:
            return value
    return None
