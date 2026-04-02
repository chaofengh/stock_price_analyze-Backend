"""
summary_core.py
Purpose: build the full chart/metrics summary for a ticker.
"""
import pandas as pd
from .data_preparation import prepare_stock_data
from .event_detection_analysis import get_touch_and_hug_events, compute_bounces_and_pullbacks
from .metrics_calculation import compute_aggregates
from .chart_builder import build_chart_data
from .data_fetcher_utils import normalize_symbol, symbol_candidates

_SUMMARY_PERIOD = "1y"
_RANGE_KEYS = ("1M", "3M", "YTD", "1Y")


def _load_summary_frame(symbol: str) -> pd.DataFrame:
    for candidate in symbol_candidates(symbol):
        data_dict = prepare_stock_data(
            candidate,
            include_rsi=False,
            period=_SUMMARY_PERIOD,
            interval="1d",
        )
        df = data_dict.get(candidate)
        if df is not None and not df.empty and "close" in df.columns:
            return df
    return pd.DataFrame()


def _trim_window(results: dict) -> dict:
    return {
        "lower_touch_bounces": results.get("lower_touch_bounces", []),
        "upper_touch_pullbacks": results.get("upper_touch_pullbacks", []),
    }


def _trim_aggregates(aggregates: dict) -> dict:
    keys = [
        "upper_touch_accuracy",
        "avg_upper_touch_drop",
        "upper_touch_count",
        "avg_upper_touch_in_days",
        "lower_touch_accuracy",
        "avg_lower_touch_bounce",
        "lower_touch_count",
        "avg_lower_touch_bounce_in_days",
    ]
    return {key: aggregates.get(key) for key in keys}


def _empty_avg_consecutive_touch_days() -> dict:
    return {key: {"upper": None, "lower": None} for key in _RANGE_KEYS}


def _to_timestamp(value) -> pd.Timestamp | None:
    if value is None:
        return None
    try:
        ts = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(ts):
        return None
    return ts


def _range_start_timestamp(latest_ts: pd.Timestamp, range_key: str) -> pd.Timestamp:
    if range_key == "YTD":
        return pd.Timestamp(
            year=latest_ts.year,
            month=1,
            day=1,
            tz=latest_ts.tz,
        )
    if range_key == "1M":
        return latest_ts - pd.DateOffset(months=1)
    if range_key == "3M":
        return latest_ts - pd.DateOffset(months=3)
    if range_key == "1Y":
        return latest_ts - pd.DateOffset(years=1)
    return latest_ts


def _touch_sequence_value(touch: dict) -> int:
    session_index = touch.get("session_index")
    if session_index is not None and session_index == session_index:
        try:
            return int(session_index)
        except (TypeError, ValueError):
            pass
    return int(touch["index"])


def _average_streak_length_for_range(
    band_touches: list[dict],
    range_start_ts: pd.Timestamp,
) -> float | None:
    streak_count = 0
    total_len = 0
    prev_seq = None
    current_len = 0

    for touch in band_touches:
        touch_ts = touch["touch_ts"]
        if touch_ts is not None and touch_ts < range_start_ts:
            continue

        seq = touch["sequence"]
        if current_len == 0:
            current_len = 1
        elif prev_seq is not None and seq == prev_seq + 1:
            current_len += 1
        else:
            streak_count += 1
            total_len += current_len
            current_len = 1

        prev_seq = seq

    if current_len > 0:
        streak_count += 1
        total_len += current_len

    if streak_count == 0:
        return None
    return total_len / streak_count


def _compute_avg_consecutive_touch_days(df: pd.DataFrame, touches: list[dict]) -> dict:
    if df.empty or "date" not in df.columns:
        return _empty_avg_consecutive_touch_days()

    latest_ts = _to_timestamp(df["date"].iloc[-1])
    if latest_ts is None:
        return _empty_avg_consecutive_touch_days()

    range_starts = {
        range_key: _range_start_timestamp(latest_ts, range_key)
        for range_key in _RANGE_KEYS
    }

    touches_by_band = {"upper": [], "lower": []}
    for touch in touches:
        band = touch.get("band")
        if band not in touches_by_band:
            continue
        touches_by_band[band].append(
            {
                "sequence": _touch_sequence_value(touch),
                "touch_ts": _to_timestamp(touch.get("date")),
            }
        )

    for band in touches_by_band:
        touches_by_band[band].sort(key=lambda item: item["sequence"])

    result = _empty_avg_consecutive_touch_days()
    for range_key in _RANGE_KEYS:
        start_ts = range_starts[range_key]
        result[range_key] = {
            "upper": _average_streak_length_for_range(touches_by_band["upper"], start_ts),
            "lower": _average_streak_length_for_range(touches_by_band["lower"], start_ts),
        }
    return result


def get_summary(symbol: str) -> dict:
    symbol = normalize_symbol(symbol)

    df = _load_summary_frame(symbol)
    if df.empty:
        raise ValueError(f"No data found for symbol {symbol}")

    initial_price = float(df["close"].iloc[0])
    final_price = float(df["close"].iloc[-1])

    touches, hug_events_upper, hug_events_lower = get_touch_and_hug_events(
        df,
        include_hugs=False,
    )
    avg_consecutive_touch_days = _compute_avg_consecutive_touch_days(df, touches)

    results_window_5 = compute_bounces_and_pullbacks(
        df, touches, hug_events_upper, hug_events_lower, window=5
    )
    results_window_10 = compute_bounces_and_pullbacks(
        df, touches, hug_events_upper, hug_events_lower, window=10
    )

    aggregated_window_5 = compute_aggregates(
        results_window_5, hug_events_upper, hug_events_lower
    )
    aggregated_window_10 = compute_aggregates(
        results_window_10, hug_events_upper, hug_events_lower
    )

    chart_data = build_chart_data(df, touches)

    return {
        "symbol": symbol,
        "final_price": final_price,
        "price_change_in_dollars": final_price - initial_price,
        "chart_data": chart_data,
        "window_5": _trim_window(results_window_5),
        "window_10": _trim_window(results_window_10),
        "aggregated_window_5": _trim_aggregates(aggregated_window_5),
        "aggregated_window_10": _trim_aggregates(aggregated_window_10),
        "avg_consecutive_touch_days": avg_consecutive_touch_days,
    }
