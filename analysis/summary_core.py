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
    }
