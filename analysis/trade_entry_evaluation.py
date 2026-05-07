"""
trade_entry_evaluation.py
Purpose: supervised Bollinger-touch continuation/reversal decision layer
with 5-day and 10-day one-year backtests.
"""
from __future__ import annotations

from copy import deepcopy
from functools import lru_cache
import math
import re
from typing import Any

import numpy as np
import pandas as pd
import talib
import yfinance as yf

from .data_preparation import prepare_stock_data
from .data_fetcher_utils import normalize_symbol, symbol_candidates

_EPS = 1e-9
_PREDICTION_THRESHOLD = 0.75
_CONTINUATION_DEPLOYMENT_THRESHOLD = 0.85
_REVERSAL_DEPLOYMENT_THRESHOLD = 0.85
_ENTRY_CONTEXT_CACHE_SIZE = 128
_HORIZONS = (5, 10)
_BACKTEST_LOOKBACK_DAYS = 365
_TRAINING_HISTORY_PERIOD = "2y"
_MIN_TRAINING_SAMPLES = 12
_MIN_VALIDATION_SAMPLES = 6
_MAX_VALIDATION_SAMPLES = 64
_MIN_VALIDATION_ACCURACY = 0.55
_CONTINUATION_PRECISION_THRESHOLD = 0.75
_REVERSAL_PRECISION_THRESHOLD = 0.85
_MIN_CONTINUATION_VALIDATION_CALLS = 6
_MIN_REVERSAL_VALIDATION_CALLS = 8
_DIRECTION_ATR_THRESHOLD = 0.5
_MODEL_ITERATIONS = 500
_MODEL_LEARNING_RATE = 0.12
_MODEL_L2 = 0.06
_MODEL_FEATURE_CLIP = 5.0
_LOGISTIC_LOOKBACKS = (None, 128, 64)
_RBF_SPECS = ((128, 1.25), (64, 2.0))
_ANALOG_K_VALUES = (8, 12, 16, 24)
_ANALOG_MIN_REVERSE_PRECISION = 0.80
_ANALOG_MIN_CONTINUE_PRECISION = 0.75
_ANALOG_MIN_REVERSE_POSTERIOR = 0.72
_ANALOG_MIN_CONTINUE_POSTERIOR = 0.70
_DEPLOY_MIN_BACKTEST_CALLS = 3
_DEPLOY_MIN_SIGNAL_BACKTEST_CALLS = 2
_DEPLOY_MIN_BACKTEST_ACCURACY = 0.75
_DEPLOY_MIN_BACKTEST_REVERSE_ACCURACY = 0.85
_DEPLOY_MIN_BACKTEST_CONTINUE_ACCURACY = 0.75
_COVERAGE_EXPANSION_MIN_SIGNAL_ACCURACY = 0.50
_MAX_EXACT_COVERAGE_EXPANSION_CANDIDATES = 18
_COVERAGE_POLICY_MAX_SAFE = "max_safe_accuracy_preserving"
_COVERAGE_REPAIR_MIN_MATCHES = 3
_COVERAGE_REPAIR_MIN_PRECISION = 0.85
_COVERAGE_REPAIR_MIN_WILSON = 0.42
_PLAYBOOK_MIN_MATCHES = 3
_PLAYBOOK_MIN_PRECISION = 0.62
_PLAYBOOK_MIN_POSTERIOR = 0.62
_PLAYBOOK_MIN_WILSON = 0.42
_ADAPTIVE_MIN_TRAINING_ROWS = 60
_ADAPTIVE_VALIDATION_LOOKBACK_ROWS = 260
_ADAPTIVE_MAX_VALIDATION_TOUCHES = 80
_ADAPTIVE_MIN_REVERSAL_VALIDATION_CALLS = 3
_ADAPTIVE_MIN_CONTINUATION_VALIDATION_CALLS = 4
_ADAPTIVE_REVERSAL_PRECISION_THRESHOLD = 0.85
_ADAPTIVE_CONTINUATION_PRECISION_THRESHOLD = 0.75
_ADAPTIVE_MIN_WILSON = 0.58
_ADAPTIVE_EXPANSION_REVERSAL_MIN_PRECISION = 0.90
_ADAPTIVE_EXPANSION_CONTINUATION_MIN_PRECISION = 0.80
_ADAPTIVE_EXPANSION_REVERSAL_MIN_WILSON = 0.70
_ADAPTIVE_EXPANSION_CONTINUATION_MIN_WILSON = 0.56
_ADAPTIVE_EXPANSION_REVERSAL_MIN_CONFIDENCE = 0.90
_ADAPTIVE_EXPANSION_CONTINUATION_MIN_CONFIDENCE = 0.82
_ADAPTIVE_OPPORTUNITY_CONTINUATION_MIN_PRECISION = 0.80
_ADAPTIVE_OPPORTUNITY_CONTINUATION_MIN_WILSON = 0.54
_ADAPTIVE_OPPORTUNITY_CONTINUATION_MIN_CONFIDENCE = 0.82
_EMPIRICAL_REGIME_MAX_MATCHES = 90
_EMPIRICAL_REGIME_RECENT_MATCHES = 10
_EMPIRICAL_REVERSAL_MIN_PRECISION = 0.86
_EMPIRICAL_CONTINUATION_MIN_PRECISION = 0.78
_EMPIRICAL_REVERSAL_MIN_WILSON = 0.62
_EMPIRICAL_CONTINUATION_MIN_WILSON = 0.54
_EMPIRICAL_REVERSAL_MIN_MATCHES = 5
_EMPIRICAL_CONTINUATION_MIN_MATCHES = 6
_EMPIRICAL_RECENT_WINDOWS = (6, 8, 12, 16)
_EMPIRICAL_RECENT_REVERSAL_MIN_PRECISION = 0.875
_EMPIRICAL_RECENT_CONTINUATION_MIN_PRECISION = 0.80
_EMPIRICAL_RECENT_REVERSAL_MIN_WILSON = 0.58
_EMPIRICAL_RECENT_CONTINUATION_MIN_WILSON = 0.50


class _NoDeepcopyDict(dict):
    """Cache holder that prevents Pandas row slicing from deep-copying model state."""

    def __deepcopy__(self, memo):
        return self


_ADAPTIVE_ANALOG_PROFILES = [
    {"id": "compact_6x20", "top_features": 6, "neighbors": 20, "same_side": False, "reversal_confidence": 0.85, "continuation_confidence": 0.80},
    {"id": "same_side_8x8", "top_features": 8, "neighbors": 8, "same_side": True, "reversal_confidence": 0.90, "continuation_confidence": 0.80},
    {"id": "broad_24x16", "top_features": 24, "neighbors": 16, "same_side": False, "reversal_confidence": 0.80, "continuation_confidence": 0.80},
    {"id": "strict_24x20", "top_features": 24, "neighbors": 20, "same_side": False, "reversal_confidence": 0.85, "continuation_confidence": 0.85},
    {"id": "balanced_16x16", "top_features": 16, "neighbors": 16, "same_side": False, "reversal_confidence": 0.85, "continuation_confidence": 0.85},
    {"id": "same_side_8x10", "top_features": 8, "neighbors": 10, "same_side": True, "reversal_confidence": 0.90, "continuation_confidence": 0.85},
    {"id": "strict_same_side_4x10", "top_features": 4, "neighbors": 10, "same_side": True, "reversal_confidence": 0.95, "continuation_confidence": 0.85},
    {"id": "same_side_12x12", "top_features": 12, "neighbors": 12, "same_side": True, "reversal_confidence": 0.85, "continuation_confidence": 0.80},
    {"id": "broad_24x12", "top_features": 24, "neighbors": 12, "same_side": False, "reversal_confidence": 0.80, "continuation_confidence": 0.75},
    {"id": "strict_6x10", "top_features": 6, "neighbors": 10, "same_side": False, "reversal_confidence": 0.90, "continuation_confidence": 0.80},
    {"id": "trend_8x24", "top_features": 8, "neighbors": 24, "same_side": False, "reversal_confidence": 0.92, "continuation_confidence": 0.78, "allowed_directions": ("continuation",)},
    {"id": "trend_12x24", "top_features": 12, "neighbors": 24, "same_side": False, "reversal_confidence": 0.92, "continuation_confidence": 0.78, "allowed_directions": ("continuation",)},
    {"id": "same_side_trend_6x16", "top_features": 6, "neighbors": 16, "same_side": True, "reversal_confidence": 0.92, "continuation_confidence": 0.80, "allowed_directions": ("continuation",)},
    {"id": "broad_consensus_32x24", "top_features": 32, "neighbors": 24, "same_side": False, "reversal_confidence": 0.92, "continuation_confidence": 0.78, "allowed_directions": ("continuation",)},
]

_ANALOG_FEATURES = [
    "touch_side_sign",
    "analysis_side_sign",
    "touch_reentry_signal",
    "touch_wick_minus_body",
    "side_close_location",
    "touch_depth_atr",
    "consecutive_touch_count",
    "side_ret_1d",
    "side_ret_3d",
    "side_ret_5d",
    "side_ret_10d",
    "side_ret_20d",
    "side_dist_ma20_atr",
    "side_ma20_slope_5",
    "side_adx_slope_5",
    "ADX14",
    "band_width_percentile",
    "bandwidth_change_5d",
    "range_expansion_5",
    "rel_volume_20",
    "side_weighted_volume_pressure_5",
    "side_rsi_deviation",
    "side_mfi_deviation",
    "side_qqq_ret_5d",
    "side_xlk_ret_5d",
    "side_qqq_dist_ma20_atr",
    "side_xlk_dist_ma20_atr",
]

_MODEL_FEATURES = [
    "touch_side_sign",
    "analysis_side_sign",
    "qqq_ret_1d",
    "qqq_ret_5d",
    "qqq_ma20_slope_5",
    "qqq_dist_ma20_atr",
    "qqq_rsi_deviation",
    "qqq_band_position",
    "side_qqq_ret_5d",
    "side_qqq_dist_ma20_atr",
    "xlk_ret_1d",
    "xlk_ret_5d",
    "xlk_ma20_slope_5",
    "xlk_dist_ma20_atr",
    "xlk_rsi_deviation",
    "xlk_band_position",
    "side_xlk_ret_5d",
    "side_xlk_dist_ma20_atr",
    "touch_reentry_signal",
    "side_close_location",
    "touch_wick_ratio",
    "touch_wick_minus_body",
    "body_pct",
    "ADX14",
    "side_ma20_slope_5",
    "side_ma50_slope_5",
    "side_macd_hist_atr",
    "side_directional_streak",
    "consecutive_touch_count",
    "volume_range_interaction",
    "rel_volume_20",
    "volume_zscore_20",
    "side_weighted_volume_pressure_5",
    "side_obv_slope_5",
    "bandwidth_change_3d",
    "band_width_percentile",
    "realized_vol_percentile",
    "side_rsi_deviation",
    "side_mfi_deviation",
    "side_cci20",
    "side_gap_atr",
    "range_expansion_5",
    "trend_alignment",
    "side_dist_ma20_atr",
    "side_dist_ma50_atr",
    "side_ma20_minus_ma50_atr",
    "side_ema10_slope_3",
    "side_ema20_slope_5",
    "side_ema50_slope_10",
    "side_wma20_slope_5",
    "side_kama20_slope_5",
    "side_tema20_slope_5",
    "side_dist_ema10_atr",
    "side_dist_ema20_atr",
    "side_dist_kama20_atr",
    "side_dist_ht_trendline_atr",
    "side_ret_1d",
    "side_ret_2d",
    "side_ret_3d",
    "side_ret_5d",
    "side_ret_10d",
    "side_ret_20d",
    "side_ret_1d_zscore_20",
    "side_ret_5d_zscore_60",
    "side_roc10",
    "side_roc20",
    "side_mom10_atr",
    "side_ppo",
    "side_cmo14",
    "side_trix15",
    "side_aroonosc14",
    "side_plus_minus_di",
    "side_adx_slope_5",
    "side_stoch_k_deviation",
    "side_stoch_d_deviation",
    "side_stochrsi_k_deviation",
    "side_stochrsi_d_deviation",
    "side_willr14_deviation",
    "side_ultosc_deviation",
    "side_bop",
    "side_pct_b_from_mid",
    "pct_b_change_1d",
    "pct_b_change_3d",
    "bandwidth_change_5d",
    "band_width_zscore_60",
    "squeeze_rank_120",
    "touch_depth_atr",
    "side_distance_to_middle_atr",
    "side_donchian20_position",
    "side_donchian55_position",
    "donchian20_width_atr",
    "donchian55_width_atr",
    "rel_volume_5",
    "rel_volume_60",
    "volume_zscore_60",
    "dollar_volume_zscore_60",
    "side_ad_slope_5",
    "side_ad_slope_10",
    "side_adosc_volume",
    "side_obv_slope_10",
    "side_up_down_volume_log_5",
    "side_body_direction_atr",
    "side_intraday_return",
    "true_range_atr",
    "true_range_percentile",
    "atr_percentile",
    "natr_percentile",
    "range_zscore_20",
    "range_zscore_60",
    "side_high_low_breakout_20",
    "side_high_low_breakout_55",
    "side_rsi_slope_5",
    "side_mfi_slope_5",
    "inside_bar",
    "outside_bar",
    "event_risk_blocked",
]


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _safe_num(value: Any, default: float = 0.0) -> float:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return default
    if not np.isfinite(num):
        return default
    return num


def _safe_bool(value: Any) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if value is None:
        return False
    try:
        if isinstance(value, float) and not np.isfinite(value):
            return False
    except Exception:
        pass
    return bool(value)


def _sigmoid(value: float) -> float:
    v = _clamp(value, -30.0, 30.0)
    return 1.0 / (1.0 + math.exp(-v))


def _to_date_string(value: Any) -> str | None:
    try:
        ts = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(ts):
        return None
    return ts.strftime("%Y-%m-%d")


def _parse_as_of_date(value: Any) -> pd.Timestamp | None:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        ts = value
    else:
        text = str(value).strip()
        if not text:
            return None
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text) is None:
            raise ValueError(f"Invalid as_of_date '{value}'. Expected YYYY-MM-DD.")
        try:
            ts = pd.Timestamp(text)
        except (TypeError, ValueError):
            raise ValueError(f"Invalid as_of_date '{value}'. Expected YYYY-MM-DD.") from None
    if pd.isna(ts):
        raise ValueError(f"Invalid as_of_date '{value}'. Expected YYYY-MM-DD.")
    if ts.tzinfo is not None:
        ts = ts.tz_convert(None)
    return ts.normalize()


def _resolve_as_of_index(df: pd.DataFrame, as_of_date: pd.Timestamp | None) -> tuple[int, bool]:
    if df.empty:
        raise ValueError("No data available to resolve as_of_date.")
    if as_of_date is None:
        return len(df) - 1, False

    date_values = pd.DatetimeIndex(df["date"])
    valid_idx = np.where(date_values <= as_of_date)[0]
    if len(valid_idx) == 0:
        first = _to_date_string(date_values[0])
        last = _to_date_string(date_values[-1])
        raise ValueError(
            f"as_of_date {as_of_date.strftime('%Y-%m-%d')} is outside available range {first} to {last}."
        )

    resolved_idx = int(valid_idx[-1])
    resolved_date = pd.Timestamp(date_values[resolved_idx]).normalize()
    snapped = resolved_date != as_of_date
    return resolved_idx, snapped


def _normalize_dates(series: pd.Series) -> pd.Series:
    dates = pd.to_datetime(series, errors="coerce")
    if getattr(dates.dt, "tz", None) is not None:
        dates = dates.dt.tz_convert(None)
    return dates.dt.normalize()


def _value_for_payload(value: Any) -> Any:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, str):
        return value
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(num):
        return None
    return round(num, 6)


def _touch_side_for_row(row: pd.Series) -> str | None:
    high = _safe_num(row.get("high"), np.nan)
    low = _safe_num(row.get("low"), np.nan)
    upper = _safe_num(row.get("BB_upper"), np.nan)
    lower = _safe_num(row.get("BB_lower"), np.nan)

    if not np.isfinite(high) or not np.isfinite(low):
        return None
    if not np.isfinite(upper) or not np.isfinite(lower):
        return None

    upper_breach = high - upper
    lower_breach = lower - low

    touched_upper = upper_breach >= 0
    touched_lower = lower_breach >= 0

    if touched_upper and touched_lower:
        return "Upper" if upper_breach >= lower_breach else "Lower"
    if touched_upper:
        return "Upper"
    if touched_lower:
        return "Lower"
    return None


def _setup_type(touched_side: str | None) -> str:
    if touched_side == "Upper":
        return "upper_band_touch"
    if touched_side == "Lower":
        return "lower_band_touch"
    return "no_band_setup"


def _classify_setup(touched_side: str | None, _legacy_flag: bool | None = None) -> str:
    """Backward-compatible private helper for older callers/tests."""
    return _setup_type(touched_side)


def _side_sign(touched_side: str | None) -> float:
    if touched_side == "Upper":
        return 1.0
    if touched_side == "Lower":
        return -1.0
    return 0.0


def _compute_signed_streak(returns: pd.Series) -> pd.Series:
    out: list[int] = []
    streak = 0
    for value in returns.fillna(0.0).tolist():
        if value > 0:
            sign = 1
        elif value < 0:
            sign = -1
        else:
            sign = 0

        if sign == 0:
            streak = 0
        elif streak == 0 or (streak > 0 and sign > 0) or (streak < 0 and sign < 0):
            streak += sign
        else:
            streak = sign

        out.append(streak)

    return pd.Series(out, index=returns.index, dtype=float)


def _consecutive_touch_counts(touched_side: pd.Series) -> pd.Series:
    counts: list[int] = []
    prev_side = None
    streak = 0

    for side in touched_side.tolist():
        if side is None:
            streak = 0
            prev_side = None
            counts.append(0)
            continue

        if side == prev_side:
            streak += 1
        else:
            streak = 1
        prev_side = side
        counts.append(streak)

    return pd.Series(counts, index=touched_side.index, dtype=float)


def _last_value_percentile(values: np.ndarray) -> float:
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    if len(arr) == 0:
        return np.nan
    last = arr[-1]
    if not np.isfinite(last):
        return np.nan
    return float(np.mean(arr <= last))


def _expanding_percentile(series: pd.Series, min_periods: int = 20) -> pd.Series:
    return series.expanding(min_periods=min_periods).apply(_last_value_percentile, raw=True)


def _rolling_percentile(series: pd.Series, window: int, min_periods: int) -> pd.Series:
    return series.rolling(window, min_periods=min_periods).apply(_last_value_percentile, raw=True)


def _rolling_zscore(series: pd.Series, window: int, min_periods: int) -> pd.Series:
    mean = series.rolling(window, min_periods=min_periods).mean()
    std = series.rolling(window, min_periods=min_periods).std()
    return (series - mean) / (std + _EPS)


@lru_cache(maxsize=256)
def _cached_earnings_dates(symbol: str) -> tuple[pd.Timestamp, ...]:
    try:
        ticker = yf.Ticker(symbol)
        if not hasattr(ticker, "get_earnings_dates"):
            return tuple()
        earnings = ticker.get_earnings_dates(limit=48)
    except Exception:
        return tuple()

    if earnings is None or getattr(earnings, "empty", True):
        return tuple()

    try:
        idx = pd.to_datetime(earnings.index, errors="coerce")
    except Exception:
        return tuple()

    cleaned: list[pd.Timestamp] = []
    for ts in idx:
        if ts is None or pd.isna(ts):
            continue
        ts = pd.Timestamp(ts)
        if ts.tzinfo is not None:
            ts = ts.tz_convert(None)
        cleaned.append(ts.normalize())

    return tuple(sorted(set(cleaned)))


def _resolve_earnings_dates(
    symbol: str,
    min_date: pd.Timestamp,
    max_date: pd.Timestamp,
    earnings_dates: set[pd.Timestamp] | None = None,
) -> set[pd.Timestamp]:
    if earnings_dates is not None:
        out: set[pd.Timestamp] = set()
        for item in earnings_dates:
            ts = pd.Timestamp(item)
            if ts.tzinfo is not None:
                ts = ts.tz_convert(None)
            out.add(ts.normalize())
        return out

    cached = _cached_earnings_dates(symbol)
    if not cached:
        return set()

    low = min_date.normalize() - pd.Timedelta(days=7)
    high = max_date.normalize() + pd.Timedelta(days=7)
    return {d for d in cached if low <= d <= high}


def _mark_event_risk_window(dates: pd.Series, earnings_dates: set[pd.Timestamp]) -> pd.Series:
    flags = pd.Series(False, index=dates.index, dtype=bool)
    if dates.empty or not earnings_dates:
        return flags

    base = pd.DatetimeIndex(dates)
    for event_date in sorted(earnings_dates):
        pos = base.searchsorted(event_date)
        candidates: list[int] = []
        if pos < len(base):
            candidates.append(int(pos))
        if pos - 1 >= 0:
            candidates.append(int(pos - 1))

        if not candidates:
            continue

        chosen = min(candidates, key=lambda idx: abs((base[idx] - event_date).days))
        start = max(0, chosen - 1)
        end = min(len(base) - 1, chosen + 1)
        flags.iloc[start : end + 1] = True

    return flags


def _build_context_features(frame: pd.DataFrame, prefix: str) -> pd.DataFrame:
    required = {"date", "open", "high", "low", "close"}
    if frame is None or frame.empty or not required.issubset(set(frame.columns)):
        return pd.DataFrame(columns=["date"])

    ctx = frame.copy()
    ctx["date"] = _normalize_dates(ctx["date"])
    ctx = ctx.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    for col in ("open", "high", "low", "close", "volume", "BB_upper", "BB_lower", "BB_middle"):
        if col in ctx.columns:
            ctx[col] = pd.to_numeric(ctx[col], errors="coerce")
    ctx = ctx.dropna(subset=["open", "high", "low", "close"]).copy()
    if ctx.empty:
        return pd.DataFrame(columns=["date"])

    close = ctx["close"].astype(float)
    high = ctx["high"].astype(float)
    low = ctx["low"].astype(float)
    atr = talib.ATR(high.values, low.values, close.values, timeperiod=14)
    ma20 = talib.SMA(close.values, timeperiod=20)
    rsi = talib.RSI(close.values, timeperiod=14)

    bb_position = pd.Series(0.0, index=ctx.index)
    if {"BB_upper", "BB_lower"}.issubset(ctx.columns):
        bb_position = (close - ctx["BB_lower"]) / (ctx["BB_upper"] - ctx["BB_lower"] + _EPS)

    out = pd.DataFrame({"date": ctx["date"]})
    out[f"{prefix}_ret_1d"] = close.pct_change(1)
    out[f"{prefix}_ret_5d"] = close.pct_change(5)
    out[f"{prefix}_ma20_slope_5"] = (pd.Series(ma20) - pd.Series(ma20).shift(5)) / (
        pd.Series(ma20).shift(5).abs() + _EPS
    )
    out[f"{prefix}_dist_ma20_atr"] = (close - ma20) / (atr + _EPS)
    out[f"{prefix}_rsi_deviation"] = (rsi - 50.0) / 30.0
    out[f"{prefix}_band_position"] = (bb_position - 0.5) * 2.0
    return out


def _prepare_feature_frame(
    frame: pd.DataFrame,
    *,
    symbol: str,
    earnings_dates: set[pd.Timestamp] | None = None,
    context_frames: dict[str, pd.DataFrame] | None = None,
) -> pd.DataFrame:
    df = frame.copy()

    required = ["date", "open", "high", "low", "close", "volume", "BB_upper", "BB_lower", "BB_middle"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns for decision layer: {missing}")

    df["date"] = _normalize_dates(df["date"])
    df = df.dropna(subset=["date"]).copy()
    df = df.sort_values("date").reset_index(drop=True)

    numeric_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "BB_upper",
        "BB_lower",
        "BB_middle",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["open", "high", "low", "close", "BB_upper", "BB_lower", "BB_middle"]).copy()
    if df.empty:
        raise ValueError("No valid rows after cleaning feature frame.")

    context_status: dict[str, bool] = {}
    if context_frames:
        for label, prefix in (("QQQ", "qqq"), ("XLK", "xlk")):
            context_feature_df = _build_context_features(context_frames.get(label), prefix)
            context_status[prefix] = not context_feature_df.empty
            if not context_feature_df.empty:
                df = df.merge(context_feature_df, on="date", how="left")
    for prefix in ("qqq", "xlk"):
        context_status.setdefault(prefix, False)
        for suffix in ("ret_1d", "ret_5d", "ma20_slope_5", "dist_ma20_atr", "rsi_deviation", "band_position"):
            col = f"{prefix}_{suffix}"
            if col not in df.columns:
                df[col] = 0.0
            else:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df = df.copy()
    df.attrs["context_status"] = context_status

    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    open_ = df["open"].astype(float)
    volume = df["volume"].fillna(0.0).astype(float)

    df["ATR14"] = talib.ATR(high.values, low.values, close.values, timeperiod=14)
    df["ADX14"] = talib.ADX(high.values, low.values, close.values, timeperiod=14)
    df["RSI14"] = talib.RSI(close.values, timeperiod=14)
    macd, macd_signal, macd_hist = talib.MACD(close.values, fastperiod=12, slowperiod=26, signalperiod=9)
    df["MACD"] = macd
    df["MACD_signal"] = macd_signal
    df["MACD_hist"] = macd_hist
    df["MACD_hist_atr"] = df["MACD_hist"] / (df["ATR14"] + _EPS)
    df["CCI20"] = talib.CCI(high.values, low.values, close.values, timeperiod=20)
    df["MFI14"] = talib.MFI(high.values, low.values, close.values, volume.values, timeperiod=14)
    df["MA20"] = talib.SMA(close.values, timeperiod=20)
    df["MA50"] = talib.SMA(close.values, timeperiod=50)
    df["EMA10"] = talib.EMA(close.values, timeperiod=10)
    df["EMA20"] = talib.EMA(close.values, timeperiod=20)
    df["EMA50"] = talib.EMA(close.values, timeperiod=50)
    df["WMA20"] = talib.WMA(close.values, timeperiod=20)
    df["DEMA20"] = talib.DEMA(close.values, timeperiod=20)
    df["TEMA20"] = talib.TEMA(close.values, timeperiod=20)
    df["KAMA20"] = talib.KAMA(close.values, timeperiod=20)
    df["T3_20"] = talib.T3(close.values, timeperiod=20)
    df["TRIMA20"] = talib.TRIMA(close.values, timeperiod=20)
    df["HT_TRENDLINE"] = talib.HT_TRENDLINE(close.values)
    df["NATR14"] = talib.NATR(high.values, low.values, close.values, timeperiod=14)
    df["PLUS_DI14"] = talib.PLUS_DI(high.values, low.values, close.values, timeperiod=14)
    df["MINUS_DI14"] = talib.MINUS_DI(high.values, low.values, close.values, timeperiod=14)
    df["DX14"] = talib.DX(high.values, low.values, close.values, timeperiod=14)
    df["MOM10"] = talib.MOM(close.values, timeperiod=10)
    df["ROC10"] = talib.ROC(close.values, timeperiod=10)
    df["ROC20"] = talib.ROC(close.values, timeperiod=20)
    df["PPO"] = talib.PPO(close.values, fastperiod=12, slowperiod=26, matype=0)
    df["CMO14"] = talib.CMO(close.values, timeperiod=14)
    df["TRIX15"] = talib.TRIX(close.values, timeperiod=15)
    stoch_k, stoch_d = talib.STOCH(
        high.values,
        low.values,
        close.values,
        fastk_period=14,
        slowk_period=3,
        slowk_matype=0,
        slowd_period=3,
        slowd_matype=0,
    )
    df["STOCH_k"] = stoch_k
    df["STOCH_d"] = stoch_d
    stochrsi_k, stochrsi_d = talib.STOCHRSI(
        close.values,
        timeperiod=14,
        fastk_period=5,
        fastd_period=3,
        fastd_matype=0,
    )
    df["STOCHRSI_k"] = stochrsi_k
    df["STOCHRSI_d"] = stochrsi_d
    df["WILLR14"] = talib.WILLR(high.values, low.values, close.values, timeperiod=14)
    df["ULTOSC"] = talib.ULTOSC(
        high.values,
        low.values,
        close.values,
        timeperiod1=7,
        timeperiod2=14,
        timeperiod3=28,
    )
    df["AROONOSC14"] = talib.AROONOSC(high.values, low.values, timeperiod=14)
    df["BOP"] = talib.BOP(open_.values, high.values, low.values, close.values)
    df["AD"] = talib.AD(high.values, low.values, close.values, volume.values)
    df["ADOSC"] = talib.ADOSC(high.values, low.values, close.values, volume.values, fastperiod=3, slowperiod=10)

    prev_close = close.shift(1)
    true_range = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    candle_range = (high - low).clip(lower=0.0)
    body = (close - open_).abs()
    upper_wick = (high - np.maximum(open_, close)).clip(lower=0.0)
    lower_wick = (np.minimum(open_, close) - low).clip(lower=0.0)

    df["body_pct"] = body / (candle_range + _EPS)
    df["body_direction_atr"] = (close - open_) / (df["ATR14"] + _EPS)
    df["intraday_return"] = (close - open_) / (open_.abs() + _EPS)
    df["upper_wick_ratio"] = upper_wick / (candle_range + _EPS)
    df["lower_wick_ratio"] = lower_wick / (candle_range + _EPS)
    df["close_in_range"] = ((close - low) / (candle_range + _EPS)).clip(0.0, 1.0)
    df["signed_close_location"] = (df["close_in_range"] - 0.5) * 2.0

    df["gap"] = open_ - prev_close
    df["gap_atr"] = df["gap"] / (df["ATR14"] + _EPS)

    df["inside_bar"] = ((high <= high.shift(1)) & (low >= low.shift(1))).astype(float)
    df["outside_bar"] = ((high >= high.shift(1)) & (low <= low.shift(1))).astype(float)

    df["true_range"] = true_range
    df["true_range_atr"] = true_range / (df["ATR14"] + _EPS)
    df["range_expansion_5"] = candle_range / (candle_range.shift(1).rolling(5, min_periods=2).mean() + _EPS)
    df["range_zscore_20"] = _rolling_zscore(candle_range, 20, 10)
    df["range_zscore_60"] = _rolling_zscore(candle_range, 60, 20)

    df["band_width"] = (df["BB_upper"] - df["BB_lower"]) / (df["BB_middle"].abs() + _EPS)
    df["pct_b"] = (close - df["BB_lower"]) / (df["BB_upper"] - df["BB_lower"] + _EPS)
    df["distance_to_middle_atr"] = (df["BB_middle"] - close) / (df["ATR14"] + _EPS)
    df["pct_b_from_mid"] = (df["pct_b"] - 0.5) * 2.0
    df["pct_b_change_1d"] = df["pct_b"].diff(1)
    df["pct_b_change_3d"] = df["pct_b"].diff(3)
    df["bandwidth_change_1d"] = df["band_width"].diff(1)
    df["bandwidth_change_3d"] = df["band_width"].diff(3)
    df["bandwidth_change_5d"] = df["band_width"].diff(5)
    df["band_width_zscore_60"] = _rolling_zscore(df["band_width"], 60, 20)
    df["squeeze_rank_120"] = _rolling_percentile(df["band_width"], 120, 30)
    df["upper_breach_atr"] = (high - df["BB_upper"]) / (df["ATR14"] + _EPS)
    df["lower_breach_atr"] = (df["BB_lower"] - low) / (df["ATR14"] + _EPS)

    returns = close.pct_change()
    df["ret_1d"] = returns
    df["ret_2d"] = close.pct_change(2)
    df["ret_3d"] = close.pct_change(3)
    df["ret_5d"] = close.pct_change(5)
    df["ret_10d"] = close.pct_change(10)
    df["ret_20d"] = close.pct_change(20)
    df["ret_1d_zscore_20"] = _rolling_zscore(df["ret_1d"], 20, 10)
    df["ret_5d_zscore_60"] = _rolling_zscore(df["ret_5d"], 60, 20)
    df["realized_vol_20"] = returns.rolling(20, min_periods=10).std() * math.sqrt(252)

    volume_mean_5 = volume.rolling(5, min_periods=2).mean()
    volume_mean_20 = volume.rolling(20, min_periods=5).mean()
    volume_mean_60 = volume.rolling(60, min_periods=20).mean()
    df["rel_volume_5"] = volume / (volume_mean_5 + _EPS)
    df["rel_volume_20"] = volume / (volume_mean_20 + _EPS)
    df["rel_volume_60"] = volume / (volume_mean_60 + _EPS)
    vol_mean = volume_mean_20
    vol_std = volume.rolling(20, min_periods=5).std()
    df["volume_zscore_20"] = (volume - vol_mean) / (vol_std + _EPS)
    vol_mean_60 = volume_mean_60
    vol_std_60 = volume.rolling(60, min_periods=20).std()
    df["volume_zscore_60"] = (volume - vol_mean_60) / (vol_std_60 + _EPS)
    df["dollar_volume"] = close * volume
    df["dollar_volume_zscore_60"] = _rolling_zscore(df["dollar_volume"], 60, 20)

    obv = talib.OBV(close.values.astype(float), volume.values.astype(float))
    df["obv"] = obv
    df["obv_slope_5"] = (df["obv"] - df["obv"].shift(5)) / (df["obv"].shift(5).abs() + _EPS)
    df["obv_slope_10"] = (df["obv"] - df["obv"].shift(10)) / (df["obv"].shift(10).abs() + _EPS)
    df["ad_slope_5"] = (df["AD"] - df["AD"].shift(5)) / ((volume_mean_20 * 5.0) + _EPS)
    df["ad_slope_10"] = (df["AD"] - df["AD"].shift(10)) / ((volume_mean_20 * 10.0) + _EPS)
    df["adosc_volume"] = df["ADOSC"] / (volume_mean_20 + _EPS)

    up_volume = np.where(close > prev_close, volume, 0.0)
    down_volume = np.where(close < prev_close, volume, 0.0)
    up_roll = pd.Series(up_volume, index=df.index).rolling(5, min_periods=2).sum()
    down_roll = pd.Series(down_volume, index=df.index).rolling(5, min_periods=2).sum()
    df["up_down_volume_ratio_5"] = up_roll / (down_roll + _EPS)
    df["up_down_volume_log_5"] = np.log((up_roll + 1.0) / (down_roll + 1.0))
    df["volume_range_interaction"] = df["rel_volume_20"] * df["true_range_atr"]
    df["weighted_volume_pressure_5"] = (
        df["signed_close_location"] * df["rel_volume_20"]
    ).rolling(5, min_periods=2).mean()

    df = df.copy()

    df["ma20_slope_5"] = (df["MA20"] - df["MA20"].shift(5)) / (df["MA20"].shift(5).abs() + _EPS)
    df["ma50_slope_5"] = (df["MA50"] - df["MA50"].shift(5)) / (df["MA50"].shift(5).abs() + _EPS)
    df["ema10_slope_3"] = (df["EMA10"] - df["EMA10"].shift(3)) / (df["EMA10"].shift(3).abs() + _EPS)
    df["ema20_slope_5"] = (df["EMA20"] - df["EMA20"].shift(5)) / (df["EMA20"].shift(5).abs() + _EPS)
    df["ema50_slope_10"] = (df["EMA50"] - df["EMA50"].shift(10)) / (df["EMA50"].shift(10).abs() + _EPS)
    df["wma20_slope_5"] = (df["WMA20"] - df["WMA20"].shift(5)) / (df["WMA20"].shift(5).abs() + _EPS)
    df["kama20_slope_5"] = (df["KAMA20"] - df["KAMA20"].shift(5)) / (df["KAMA20"].shift(5).abs() + _EPS)
    df["tema20_slope_5"] = (df["TEMA20"] - df["TEMA20"].shift(5)) / (df["TEMA20"].shift(5).abs() + _EPS)
    df["dist_ma20_atr"] = (close - df["MA20"]) / (df["ATR14"] + _EPS)
    df["dist_ma50_atr"] = (close - df["MA50"]) / (df["ATR14"] + _EPS)
    df["dist_ema10_atr"] = (close - df["EMA10"]) / (df["ATR14"] + _EPS)
    df["dist_ema20_atr"] = (close - df["EMA20"]) / (df["ATR14"] + _EPS)
    df["dist_kama20_atr"] = (close - df["KAMA20"]) / (df["ATR14"] + _EPS)
    df["dist_ht_trendline_atr"] = (close - df["HT_TRENDLINE"]) / (df["ATR14"] + _EPS)
    df["ma20_minus_ma50_atr"] = (df["MA20"] - df["MA50"]) / (df["ATR14"] + _EPS)
    df["adx_slope_5"] = (df["ADX14"] - df["ADX14"].shift(5)) / 25.0
    df["plus_minus_di"] = (df["PLUS_DI14"] - df["MINUS_DI14"]) / 50.0
    df["rsi_slope_5"] = (df["RSI14"] - df["RSI14"].shift(5)) / 30.0
    df["mfi_slope_5"] = (df["MFI14"] - df["MFI14"].shift(5)) / 30.0

    streak = _compute_signed_streak(returns)
    df["signed_streak"] = streak
    df["directional_streak"] = (streak / 5.0).clip(-1.0, 1.0)

    df["band_width_percentile"] = _expanding_percentile(df["band_width"])
    df["realized_vol_percentile"] = _expanding_percentile(df["realized_vol_20"])
    df["true_range_percentile"] = _expanding_percentile(df["true_range_atr"])
    df["atr_percentile"] = _expanding_percentile(df["ATR14"])
    df["natr_percentile"] = _expanding_percentile(df["NATR14"])

    donchian20_high = high.rolling(20, min_periods=5).max()
    donchian20_low = low.rolling(20, min_periods=5).min()
    donchian55_high = high.rolling(55, min_periods=20).max()
    donchian55_low = low.rolling(55, min_periods=20).min()
    prev20_high = high.shift(1).rolling(20, min_periods=5).max()
    prev20_low = low.shift(1).rolling(20, min_periods=5).min()
    prev55_high = high.shift(1).rolling(55, min_periods=20).max()
    prev55_low = low.shift(1).rolling(55, min_periods=20).min()
    df["donchian20_position"] = (close - donchian20_low) / (donchian20_high - donchian20_low + _EPS)
    df["donchian55_position"] = (close - donchian55_low) / (donchian55_high - donchian55_low + _EPS)
    df["donchian20_width_atr"] = (donchian20_high - donchian20_low) / (df["ATR14"] + _EPS)
    df["donchian55_width_atr"] = (donchian55_high - donchian55_low) / (df["ATR14"] + _EPS)
    df["upper_breakout_20_atr"] = (high - prev20_high) / (df["ATR14"] + _EPS)
    df["lower_breakout_20_atr"] = (prev20_low - low) / (df["ATR14"] + _EPS)
    df["upper_breakout_55_atr"] = (high - prev55_high) / (df["ATR14"] + _EPS)
    df["lower_breakout_55_atr"] = (prev55_low - low) / (df["ATR14"] + _EPS)

    df["touched_side"] = df.apply(_touch_side_for_row, axis=1)
    df["consecutive_touch_count"] = _consecutive_touch_counts(df["touched_side"])
    df["touch_side_sign"] = df["touched_side"].map({"Upper": 1.0, "Lower": -1.0}).fillna(0.0)
    df["analysis_side"] = np.where(
        df["touched_side"].isin(["Upper", "Lower"]),
        df["touched_side"],
        np.where(df["close"] >= df["BB_middle"], "Upper", "Lower"),
    )
    df["analysis_side_sign"] = pd.Series(df["analysis_side"], index=df.index).map({"Upper": 1.0, "Lower": -1.0})
    df["touch_wick_ratio"] = np.where(
        df["touched_side"] == "Upper",
        df["upper_wick_ratio"],
        np.where(df["touched_side"] == "Lower", df["lower_wick_ratio"], 0.0),
    )
    df["touch_wick_minus_body"] = df["touch_wick_ratio"] - df["body_pct"]

    upper_reentry = np.where(df["close"] < df["BB_upper"], 1.0, -1.0)
    lower_reentry = np.where(df["close"] > df["BB_lower"], 1.0, -1.0)
    df["touch_reentry_signal"] = np.where(
        df["touched_side"] == "Upper",
        upper_reentry,
        np.where(df["touched_side"] == "Lower", lower_reentry, 0.0),
    )

    base_alignment = ((df["dist_ma20_atr"] + (df["MA20"] - df["MA50"]) / (df["ATR14"] + _EPS)) / 2.0).fillna(0.0)
    df["trend_alignment"] = np.where(
        df["touched_side"] == "Upper",
        -base_alignment,
        np.where(df["touched_side"] == "Lower", base_alignment, 0.0),
    )

    df = df.copy()

    side = df["analysis_side_sign"]
    df["side_qqq_ret_5d"] = side * df["qqq_ret_5d"]
    df["side_qqq_dist_ma20_atr"] = side * df["qqq_dist_ma20_atr"]
    df["side_xlk_ret_5d"] = side * df["xlk_ret_5d"]
    df["side_xlk_dist_ma20_atr"] = side * df["xlk_dist_ma20_atr"]
    df["side_close_location"] = side * df["signed_close_location"]
    df["side_ma20_slope_5"] = side * df["ma20_slope_5"]
    df["side_ma50_slope_5"] = side * df["ma50_slope_5"]
    df["side_macd_hist_atr"] = side * df["MACD_hist_atr"]
    df["side_directional_streak"] = side * df["directional_streak"]
    df["side_weighted_volume_pressure_5"] = side * df["weighted_volume_pressure_5"]
    df["side_obv_slope_5"] = side * df["obv_slope_5"]
    df["side_rsi_deviation"] = side * ((df["RSI14"] - 50.0) / 30.0)
    df["side_mfi_deviation"] = side * ((df["MFI14"] - 50.0) / 30.0)
    df["side_cci20"] = side * (df["CCI20"] / 200.0)
    df["side_gap_atr"] = side * df["gap_atr"]
    df["side_dist_ma20_atr"] = side * df["dist_ma20_atr"]
    df["side_dist_ma50_atr"] = side * df["dist_ma50_atr"]
    df["side_ma20_minus_ma50_atr"] = side * df["ma20_minus_ma50_atr"]
    df["side_ema10_slope_3"] = side * df["ema10_slope_3"]
    df["side_ema20_slope_5"] = side * df["ema20_slope_5"]
    df["side_ema50_slope_10"] = side * df["ema50_slope_10"]
    df["side_wma20_slope_5"] = side * df["wma20_slope_5"]
    df["side_kama20_slope_5"] = side * df["kama20_slope_5"]
    df["side_tema20_slope_5"] = side * df["tema20_slope_5"]
    df["side_dist_ema10_atr"] = side * df["dist_ema10_atr"]
    df["side_dist_ema20_atr"] = side * df["dist_ema20_atr"]
    df["side_dist_kama20_atr"] = side * df["dist_kama20_atr"]
    df["side_dist_ht_trendline_atr"] = side * df["dist_ht_trendline_atr"]
    df["side_ret_1d"] = side * df["ret_1d"]
    df["side_ret_2d"] = side * df["ret_2d"]
    df["side_ret_3d"] = side * df["ret_3d"]
    df["side_ret_5d"] = side * df["ret_5d"]
    df["side_ret_10d"] = side * df["ret_10d"]
    df["side_ret_20d"] = side * df["ret_20d"]
    df["side_ret_1d_zscore_20"] = side * df["ret_1d_zscore_20"]
    df["side_ret_5d_zscore_60"] = side * df["ret_5d_zscore_60"]
    df["side_roc10"] = side * (df["ROC10"] / 10.0)
    df["side_roc20"] = side * (df["ROC20"] / 10.0)
    df["side_mom10_atr"] = side * (df["MOM10"] / (df["ATR14"] + _EPS))
    df["side_ppo"] = side * (df["PPO"] / 10.0)
    df["side_cmo14"] = side * (df["CMO14"] / 100.0)
    df["side_trix15"] = side * (df["TRIX15"] / 5.0)
    df["side_aroonosc14"] = side * (df["AROONOSC14"] / 100.0)
    df["side_plus_minus_di"] = side * df["plus_minus_di"]
    df["side_adx_slope_5"] = side * df["adx_slope_5"]
    df["side_stoch_k_deviation"] = side * ((df["STOCH_k"] - 50.0) / 50.0)
    df["side_stoch_d_deviation"] = side * ((df["STOCH_d"] - 50.0) / 50.0)
    df["side_stochrsi_k_deviation"] = side * ((df["STOCHRSI_k"] - 50.0) / 50.0)
    df["side_stochrsi_d_deviation"] = side * ((df["STOCHRSI_d"] - 50.0) / 50.0)
    df["side_willr14_deviation"] = side * ((df["WILLR14"] + 50.0) / 50.0)
    df["side_ultosc_deviation"] = side * ((df["ULTOSC"] - 50.0) / 50.0)
    df["side_bop"] = side * df["BOP"]
    df["side_pct_b_from_mid"] = side * df["pct_b_from_mid"]
    df["touch_depth_atr"] = np.where(
        df["touched_side"] == "Upper",
        df["upper_breach_atr"],
        np.where(df["touched_side"] == "Lower", df["lower_breach_atr"], 0.0),
    )
    df["side_distance_to_middle_atr"] = side * df["distance_to_middle_atr"]
    df["side_donchian20_position"] = side * ((df["donchian20_position"] - 0.5) * 2.0)
    df["side_donchian55_position"] = side * ((df["donchian55_position"] - 0.5) * 2.0)
    df["side_ad_slope_5"] = side * df["ad_slope_5"]
    df["side_ad_slope_10"] = side * df["ad_slope_10"]
    df["side_adosc_volume"] = side * df["adosc_volume"]
    df["side_obv_slope_10"] = side * df["obv_slope_10"]
    df["side_up_down_volume_log_5"] = side * df["up_down_volume_log_5"]
    df["side_body_direction_atr"] = side * df["body_direction_atr"]
    df["side_intraday_return"] = side * df["intraday_return"]
    df["side_high_low_breakout_20"] = np.where(
        df["touched_side"] == "Upper",
        df["upper_breakout_20_atr"],
        np.where(df["touched_side"] == "Lower", df["lower_breakout_20_atr"], 0.0),
    )
    df["side_high_low_breakout_55"] = np.where(
        df["touched_side"] == "Upper",
        df["upper_breakout_55_atr"],
        np.where(df["touched_side"] == "Lower", df["lower_breakout_55_atr"], 0.0),
    )
    df["side_rsi_slope_5"] = side * df["rsi_slope_5"]
    df["side_mfi_slope_5"] = side * df["mfi_slope_5"]

    df["target_distance_atr"] = (df["BB_middle"] - close).abs() / (df["ATR14"] + _EPS)

    min_date = pd.Timestamp(df["date"].iloc[0])
    max_date = pd.Timestamp(df["date"].iloc[-1])
    earnings_set = _resolve_earnings_dates(symbol, min_date=min_date, max_date=max_date, earnings_dates=earnings_dates)
    df["event_risk_blocked"] = _mark_event_risk_window(df["date"], earnings_set)
    df.attrs["context_status"] = context_status

    return df


def _actual_direction(
    touched_side: str,
    signal_close: float,
    outcome_close: float,
    continuation_hurdle: float = 0.0,
) -> str:
    hurdle = max(0.0, _safe_num(continuation_hurdle, 0.0))
    if hurdle <= _EPS and np.isclose(outcome_close, signal_close, atol=1e-12, rtol=0.0):
        return "flat"
    if touched_side == "Upper":
        return "continuation" if outcome_close > signal_close + hurdle else "reversal"
    return "continuation" if outcome_close < signal_close - hurdle else "reversal"


def _continuation_hurdle_for_row(row: pd.Series) -> float:
    atr = _safe_num(row.get("ATR14"), np.nan)
    if not np.isfinite(atr) or atr <= 0:
        return 0.0
    return _DIRECTION_ATR_THRESHOLD * atr


def _reversal_veto_reason(row: pd.Series, horizon: int) -> str | None:
    """Block reversal calls during broad, low-rejection breakout pressure."""
    row_side = _training_side_for_row(row)
    side_qqq = _safe_num(row.get("side_qqq_ret_5d"), 0.0)
    side_xlk = _safe_num(row.get("side_xlk_ret_5d"), 0.0)
    side_ret_5d = _safe_num(row.get("side_ret_5d"), 0.0)
    side_ret_10d = _safe_num(row.get("side_ret_10d"), 0.0)
    volume_pressure = _safe_num(row.get("side_weighted_volume_pressure_5"), 0.0)
    touch_depth = _safe_num(row.get("touch_depth_atr"), 0.0)
    wick_minus_body = _safe_num(row.get("touch_wick_minus_body"), 0.0)
    consecutive_touch_count = _safe_num(row.get("consecutive_touch_count"), 0.0)
    band_width_percentile = _safe_num(row.get("band_width_percentile"), 0.0)
    bandwidth_change = _safe_num(row.get("bandwidth_change_5d"), 0.0)

    strong_context_trend = side_qqq > 0.04 and side_xlk > 0.04
    positive_context_trend = side_qqq > 0.03 and side_xlk > 0.035
    strong_symbol_thrust = side_ret_5d > 0.055 or side_ret_10d > 0.10
    weak_rejection = wick_minus_body < 0.25 and volume_pressure >= 0.0
    soft_rejection = wick_minus_body < 0.30 and volume_pressure > 0.12 and touch_depth < 0.50
    shallow_pressure_touch = touch_depth < 0.25 and volume_pressure > 0.15
    persistent_band_walk = consecutive_touch_count >= 4 and side_ret_5d > 0.07 and volume_pressure > 0.25
    expanding_breakout = band_width_percentile > 0.80 and bandwidth_change > 0.08 and volume_pressure > 0.25

    if strong_context_trend and strong_symbol_thrust and weak_rejection:
        return "breakout_pressure"
    if positive_context_trend and soft_rejection:
        return "context_pressure_rejection_too_weak"
    if shallow_pressure_touch:
        return "shallow_pressure_touch"
    if horizon >= 10 and (persistent_band_walk or expanding_breakout):
        return "persistent_breakout"

    if row_side == "Lower":
        falling_thrust = side_ret_5d > 0.04 or side_ret_10d > 0.06
        early_band_walk = consecutive_touch_count < 6
        still_outside_band = _safe_num(row.get("touch_reentry_signal"), 0.0) < 0
        no_capitulation = volume_pressure < 0.20 and wick_minus_body < 0.25
        market_not_helping = side_qqq > 0.005 or side_xlk > 0.005
        if falling_thrust and no_capitulation and (early_band_walk or still_outside_band or market_not_helping):
            return "falling_knife_no_exhaustion"
    return None


def _model_metadata(
    training_sample_count: int,
    positive_count: int = 0,
    negative_count: int = 0,
    *,
    validation_accuracy: float | None = None,
    validation_precision: float | None = None,
    candidate_count: int = 0,
) -> dict:
    return {
        "type": "blocked_walk_forward_validated_ensemble",
        "positive_label": "continuation",
        "training_scope": "all_available_days",
        "prediction_scope": "latest_day_always_or_bollinger_touch_days",
        "training_sample_count": int(training_sample_count),
        "continuation_training_count": int(positive_count),
        "reversal_training_count": int(negative_count),
        "min_training_samples": _MIN_TRAINING_SAMPLES,
        "validation_accuracy": round(validation_accuracy, 6) if validation_accuracy is not None else None,
        "validation_precision": round(validation_precision, 6) if validation_precision is not None else None,
        "continuation_precision_threshold": _CONTINUATION_PRECISION_THRESHOLD,
        "reversal_precision_threshold": _REVERSAL_PRECISION_THRESHOLD,
        "continuation_deployment_threshold": _CONTINUATION_DEPLOYMENT_THRESHOLD,
        "reversal_deployment_threshold": _REVERSAL_DEPLOYMENT_THRESHOLD,
        "min_continuation_validation_calls": _MIN_CONTINUATION_VALIDATION_CALLS,
        "min_reversal_validation_calls": _MIN_REVERSAL_VALIDATION_CALLS,
        "continuation_hurdle_atr": _DIRECTION_ATR_THRESHOLD,
        "deployment_min_backtest_calls": _DEPLOY_MIN_BACKTEST_CALLS,
        "deployment_min_backtest_accuracy": _DEPLOY_MIN_BACKTEST_ACCURACY,
        "deployment_min_backtest_reverse_accuracy": _DEPLOY_MIN_BACKTEST_REVERSE_ACCURACY,
        "deployment_min_backtest_continue_accuracy": _DEPLOY_MIN_BACKTEST_CONTINUE_ACCURACY,
        "feature_count": len(_MODEL_FEATURES),
        "candidate_search_count": len(_candidate_specs()),
        "training_history_period": _TRAINING_HISTORY_PERIOD,
        "model_iterations": _MODEL_ITERATIONS,
        "candidate_count": int(candidate_count),
    }


def _no_prediction_horizon(
    horizon: int,
    reason: str,
    *,
    training_sample_count: int = 0,
    positive_count: int = 0,
    negative_count: int = 0,
) -> dict:
    return {
        "status": "no_prediction",
        "predicted_direction": None,
        "continuation_probability": 0.5,
        "reversal_probability": 0.5,
        "continuation_confidence_score": 50,
        "reversal_confidence_score": 50,
        "continuation_validation_precision": None,
        "reversal_validation_precision": None,
        "continuation_validation_count": 0,
        "reversal_validation_count": 0,
        "validation_policy": {
            "validation_count": 0,
            "active_side": None,
            "active_validation_count": 0,
            "continuation": None,
            "reversal": None,
            "selected": None,
        },
        "confidence_score": 0,
        "threshold": _PREDICTION_THRESHOLD,
        "no_prediction_reason": reason,
        "reversal_veto_reason": None,
        "analog_evidence": None,
        "analog_override": False,
        "deployment_quality_gate": None,
        "blocked_prediction": None,
        "contributions": [],
        "model": _model_metadata(training_sample_count, positive_count, negative_count),
    }


def _feature_vector(row: pd.Series) -> np.ndarray:
    values = [_safe_num(row.get(feature), 0.0) for feature in _MODEL_FEATURES]
    vector = np.asarray(values, dtype=float)
    return np.nan_to_num(vector, nan=0.0, posinf=0.0, neginf=0.0)


def _empty_training_matrix() -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    return (
        np.empty((0, len(_MODEL_FEATURES)), dtype=float),
        np.asarray([], dtype=float),
        np.asarray([], dtype=int),
    )


def _feature_side_label(vector: np.ndarray) -> str:
    try:
        touch_side_sign = _safe_num(vector[_MODEL_FEATURES.index("touch_side_sign")], 0.0)
        analysis_side_sign = _safe_num(vector[_MODEL_FEATURES.index("analysis_side_sign")], 0.0)
    except (ValueError, IndexError):
        return "unknown"
    side_sign = touch_side_sign if abs(touch_side_sign) > _EPS else analysis_side_sign
    if side_sign > 0:
        return "upper"
    if side_sign < 0:
        return "lower"
    return "unknown"


def _analog_feature_indices() -> list[int]:
    return [_MODEL_FEATURES.index(feature) for feature in _ANALOG_FEATURES if feature in _MODEL_FEATURES]


def _wilson_lower_bound(success_count: int, sample_count: int, z_value: float = 1.0) -> float:
    if sample_count <= 0:
        return 0.0
    phat = success_count / sample_count
    denominator = 1.0 + (z_value * z_value / sample_count)
    centre = phat + (z_value * z_value / (2.0 * sample_count))
    margin = z_value * math.sqrt((phat * (1.0 - phat) + (z_value * z_value / (4.0 * sample_count))) / sample_count)
    return _clamp((centre - margin) / denominator, 0.0, 1.0)


def _training_side_for_row(row: pd.Series) -> str | None:
    touched_side = row.get("touched_side")
    if touched_side in ("Upper", "Lower"):
        return touched_side

    side_sign = _safe_num(row.get("analysis_side_sign"), 0.0)
    if side_sign > 0:
        return "Upper"
    if side_sign < 0:
        return "Lower"
    return None


def _label_for_index(feature_df: pd.DataFrame, idx: int, horizon: int) -> int | None:
    if idx + horizon >= len(feature_df):
        return None
    row = feature_df.iloc[idx]
    if _safe_bool(row.get("event_risk_blocked")):
        return None
    training_side = _training_side_for_row(row)
    if training_side not in ("Upper", "Lower"):
        return None
    signal_close = _safe_num(row.get("close"), np.nan)
    outcome_close = _safe_num(feature_df.iloc[idx + horizon].get("close"), np.nan)
    if not np.isfinite(signal_close) or not np.isfinite(outcome_close):
        return None
    direction = _actual_direction(training_side, signal_close, outcome_close, _continuation_hurdle_for_row(row))
    if direction == "flat":
        return None
    return 1 if direction == "continuation" else 0


def _build_training_matrix(
    feature_df: pd.DataFrame,
    *,
    target_idx: int,
    horizon: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    rows: list[np.ndarray] = []
    labels: list[int] = []
    indices: list[int] = []

    for idx in range(target_idx):
        # All historical bars can train the model once their horizon outcome is known.
        if idx + horizon > target_idx:
            continue
        label = _label_for_index(feature_df, idx, horizon)
        if label is None:
            continue
        rows.append(_feature_vector(feature_df.iloc[idx]))
        labels.append(label)
        indices.append(idx)

    if not rows:
        return _empty_training_matrix()
    return np.vstack(rows), np.asarray(labels, dtype=float), np.asarray(indices, dtype=int)


def _fit_logistic_arrays(x_train: np.ndarray, y_train: np.ndarray, *, kind: str, lookback: int | None) -> dict | None:
    sample_count = int(len(y_train))
    if sample_count < _MIN_TRAINING_SAMPLES:
        return None
    positive_count = int(np.sum(y_train == 1.0))
    negative_count = int(np.sum(y_train == 0.0))
    if positive_count == 0 or negative_count == 0:
        return None

    mean = x_train.mean(axis=0)
    scale = x_train.std(axis=0)
    scale = np.where(scale < _EPS, 1.0, scale)
    x_scaled = np.clip((x_train - mean) / scale, -_MODEL_FEATURE_CLIP, _MODEL_FEATURE_CLIP)

    pos_rate = _clamp(float(y_train.mean()), 0.02, 0.98)
    intercept = math.log(pos_rate / (1.0 - pos_rate))
    coef = np.zeros(x_scaled.shape[1], dtype=float)
    recency_weights = np.exp(np.linspace(-1.6, 0.0, sample_count))
    recency_weights = recency_weights / recency_weights.mean()

    for _ in range(_MODEL_ITERATIONS):
        logits = np.clip(x_scaled @ coef + intercept, -30.0, 30.0)
        probs = 1.0 / (1.0 + np.exp(-logits))
        error = (probs - y_train) * recency_weights
        grad_coef = (x_scaled.T @ error) / sample_count + (_MODEL_L2 * coef)
        grad_intercept = float(error.mean())
        coef -= _MODEL_LEARNING_RATE * grad_coef
        intercept -= _MODEL_LEARNING_RATE * grad_intercept

    return {
        "kind": kind,
        "lookback": lookback,
        "sample_count": sample_count,
        "positive_count": positive_count,
        "negative_count": negative_count,
        "mean": mean,
        "scale": scale,
        "coef": coef,
        "intercept": float(intercept),
    }


def _fit_rbf_arrays(
    x_train: np.ndarray,
    y_train: np.ndarray,
    *,
    kind: str,
    lookback: int | None,
    gamma: float,
) -> dict | None:
    sample_count = int(len(y_train))
    if sample_count < _MIN_TRAINING_SAMPLES:
        return None
    positive_count = int(np.sum(y_train == 1.0))
    negative_count = int(np.sum(y_train == 0.0))
    if positive_count == 0 or negative_count == 0:
        return None

    mean = x_train.mean(axis=0)
    scale = x_train.std(axis=0)
    scale = np.where(scale < _EPS, 1.0, scale)
    x_scaled = np.clip((x_train - mean) / scale, -_MODEL_FEATURE_CLIP, _MODEL_FEATURE_CLIP)
    recency_weights = np.exp(np.linspace(-1.8, 0.0, sample_count))
    pos_centroid = x_scaled[y_train == 1.0].mean(axis=0)
    neg_centroid = x_scaled[y_train == 0.0].mean(axis=0)

    return {
        "kind": kind,
        "lookback": lookback,
        "gamma": gamma,
        "sample_count": sample_count,
        "positive_count": positive_count,
        "negative_count": negative_count,
        "mean": mean,
        "scale": scale,
        "x_scaled": x_scaled,
        "y_train": y_train,
        "recency_weights": recency_weights,
        "prior": float(y_train.mean()),
        "pos_centroid": pos_centroid,
        "neg_centroid": neg_centroid,
    }


def _slice_training_records(
    x_train: np.ndarray,
    y_train: np.ndarray,
    indices: np.ndarray,
    lookback: int | None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    if lookback is not None and len(y_train) > lookback:
        return x_train[-lookback:], y_train[-lookback:], indices[-lookback:]
    return x_train, y_train, indices


def _fit_candidate(
    x_train: np.ndarray,
    y_train: np.ndarray,
    indices: np.ndarray,
    spec: dict,
) -> dict | None:
    x_slice, y_slice, idx_slice = _slice_training_records(x_train, y_train, indices, spec.get("lookback"))
    if spec["kind"] == "logistic":
        model = _fit_logistic_arrays(x_slice, y_slice, kind="logistic", lookback=spec.get("lookback"))
    else:
        model = _fit_rbf_arrays(
            x_slice,
            y_slice,
            kind="rbf",
            lookback=spec.get("lookback"),
            gamma=spec["gamma"],
        )
    if model is not None:
        model["indices"] = idx_slice
    return model


def _predict_candidate_probability(x_raw: np.ndarray, model: dict) -> float:
    x_scaled = np.clip((x_raw - model["mean"]) / model["scale"], -_MODEL_FEATURE_CLIP, _MODEL_FEATURE_CLIP)
    if model["kind"] == "logistic":
        return _sigmoid(float(x_scaled @ model["coef"] + model["intercept"]))

    diff = model["x_scaled"] - x_scaled
    distance = np.mean(diff * diff, axis=1)
    similarity = np.exp(-distance / (2.0 * model["gamma"] * model["gamma"]))
    weights = similarity * model["recency_weights"]
    weight_sum = float(weights.sum())
    if weight_sum <= _EPS:
        return _clamp(float(model["prior"]), 0.01, 0.99)
    return _clamp(float(np.dot(weights, model["y_train"]) / weight_sum), 0.01, 0.99)


def _candidate_specs() -> list[dict]:
    specs: list[dict] = [{"kind": "logistic", "lookback": lookback} for lookback in _LOGISTIC_LOOKBACKS]
    for lookback, gamma in _RBF_SPECS:
        specs.append({"kind": "rbf", "lookback": lookback, "gamma": gamma})
    return specs


def _validate_candidate(
    x_train: np.ndarray,
    y_train: np.ndarray,
    indices: np.ndarray,
    *,
    horizon: int,
    spec: dict,
) -> dict | None:
    if len(y_train) < _MIN_TRAINING_SAMPLES + _MIN_VALIDATION_SAMPLES:
        return None

    val_count = min(
        max(_MIN_VALIDATION_SAMPLES, int(round(len(y_train) * 0.35))),
        max(_MIN_VALIDATION_SAMPLES, len(y_train) // 2),
        _MAX_VALIDATION_SAMPLES,
    )
    start = max(_MIN_TRAINING_SAMPLES, len(y_train) - val_count)
    train_positions = np.asarray([], dtype=int)
    while start <= len(y_train) - _MIN_VALIDATION_SAMPLES:
        first_validation_idx = int(indices[start])
        train_positions = np.where(indices[:start] + horizon <= first_validation_idx)[0]
        if len(train_positions) >= _MIN_TRAINING_SAMPLES:
            break
        start += 1

    if len(train_positions) < _MIN_TRAINING_SAMPLES or start > len(y_train) - _MIN_VALIDATION_SAMPLES:
        return None

    candidate = _fit_candidate(
        x_train[train_positions],
        y_train[train_positions],
        indices[train_positions],
        spec,
    )
    if candidate is None:
        return None

    predictions: list[dict] = []
    for pos in range(start, len(y_train)):
        probability = _predict_candidate_probability(x_train[pos], candidate)
        predicted = 1 if probability >= 0.5 else 0
        predictions.append(
            {
                "index": int(indices[pos]),
                "predicted": predicted,
                "actual": int(y_train[pos]),
                "probability": float(probability),
                "side": _feature_side_label(x_train[pos]),
            }
        )

    if len(predictions) < _MIN_VALIDATION_SAMPLES:
        return None

    correct = sum(1 for item in predictions if item["predicted"] == item["actual"])
    brier = float(np.mean([(item["probability"] - item["actual"]) ** 2 for item in predictions]))
    continuation_preds = [item for item in predictions if item["predicted"] == 1]
    reversal_preds = [item for item in predictions if item["predicted"] == 0]
    continuation_precision = (
        sum(1 for item in continuation_preds if item["actual"] == 1) / len(continuation_preds)
        if continuation_preds
        else None
    )
    reversal_precision = (
        sum(1 for item in reversal_preds if item["actual"] == 0) / len(reversal_preds)
        if reversal_preds
        else None
    )

    return {
        "validation_count": len(predictions),
        "accuracy": correct / len(predictions),
        "brier": brier,
        "continuation_precision": continuation_precision,
        "reversal_precision": reversal_precision,
        "continuation_prediction_count": len(continuation_preds),
        "reversal_prediction_count": len(reversal_preds),
        "predictions": predictions,
    }


def _combine_validation_predictions(candidates: list[dict]) -> list[dict]:
    combined: dict[int, dict] = {}
    for candidate in candidates:
        weight = _safe_num(candidate.get("weight"), 0.0)
        if weight <= 0:
            continue
        validation = candidate.get("validation", {})
        for item in validation.get("predictions", []):
            idx = int(item.get("index", -1))
            if idx < 0:
                continue
            actual = int(item.get("actual", 0))
            probability = _clamp(_safe_num(item.get("probability"), 0.5), 0.01, 0.99)
            bucket = combined.setdefault(
                idx,
                {
                    "weighted_probability": 0.0,
                    "weight": 0.0,
                    "actual": actual,
                    "side": item.get("side", "unknown"),
                },
            )
            bucket["weighted_probability"] += weight * probability
            bucket["weight"] += weight
            bucket["actual"] = actual
            if bucket.get("side") == "unknown" and item.get("side") in ("upper", "lower"):
                bucket["side"] = item.get("side")

    rows: list[dict] = []
    for idx, item in sorted(combined.items()):
        weight = _safe_num(item.get("weight"), 0.0)
        if weight <= _EPS:
            continue
        rows.append(
            {
                "index": idx,
                "probability": _clamp(item["weighted_probability"] / weight, 0.01, 0.99),
                "actual": int(item.get("actual", 0)),
                "side": item.get("side", "unknown"),
            }
        )
    return rows


def _direction_policy_from_rows(
    rows: list[dict],
    *,
    direction: str,
    precision_target: float,
    min_calls: int,
) -> dict | None:
    if direction not in ("continuation", "reversal"):
        return None
    if not rows:
        return None

    effective_min_calls = min(min_calls, max(3, len(rows) // 2))
    probabilities = sorted({_clamp(_safe_num(row.get("probability"), 0.5), 0.01, 0.99) for row in rows})
    best: dict | None = None

    if direction == "continuation":
        for cutoff in sorted(probabilities, reverse=True):
            selected = [row for row in rows if _safe_num(row.get("probability"), 0.5) >= cutoff]
            call_count = len(selected)
            if call_count < effective_min_calls:
                continue
            correct_count = sum(1 for row in selected if int(row.get("actual", 0)) == 1)
            precision = correct_count / call_count
            if precision < precision_target:
                continue
            # Descending cutoffs means later passing cutoffs are more permissive.
            best = {
                "direction": direction,
                "cutoff": float(cutoff),
                "precision": float(precision),
                "call_count": int(call_count),
                "correct_count": int(correct_count),
                "min_call_count": int(effective_min_calls),
            }
    else:
        for cutoff in probabilities:
            selected = [row for row in rows if _safe_num(row.get("probability"), 0.5) <= cutoff]
            call_count = len(selected)
            if call_count < effective_min_calls:
                continue
            correct_count = sum(1 for row in selected if int(row.get("actual", 1)) == 0)
            precision = correct_count / call_count
            if precision < precision_target:
                continue
            # Ascending cutoffs means later passing cutoffs are more permissive.
            best = {
                "direction": direction,
                "cutoff": float(cutoff),
                "precision": float(precision),
                "call_count": int(call_count),
                "correct_count": int(correct_count),
                "min_call_count": int(effective_min_calls),
            }

    return best


def _derive_precision_policy(candidates: list[dict]) -> dict:
    rows = _combine_validation_predictions(candidates)
    groups = {
        "overall": rows,
        "upper": [row for row in rows if row.get("side") == "upper"],
        "lower": [row for row in rows if row.get("side") == "lower"],
    }
    policy: dict[str, Any] = {"validation_count": len(rows)}
    for group_name, group_rows in groups.items():
        policy[group_name] = {
            "validation_count": len(group_rows),
            "continuation": _direction_policy_from_rows(
                group_rows,
                direction="continuation",
                precision_target=_CONTINUATION_PRECISION_THRESHOLD,
                min_calls=_MIN_CONTINUATION_VALIDATION_CALLS,
            ),
            "reversal": _direction_policy_from_rows(
                group_rows,
                direction="reversal",
                precision_target=_REVERSAL_PRECISION_THRESHOLD,
                min_calls=_MIN_REVERSAL_VALIDATION_CALLS,
            ),
        }
    policy["continuation"] = policy["overall"]["continuation"]
    policy["reversal"] = policy["overall"]["reversal"]
    return policy


def _analog_meta_label(row: pd.Series, model: dict, direction: str) -> dict:
    x_train = model.get("x_train")
    y_train = model.get("y_train")
    if x_train is None or y_train is None:
        return {"status": "missing", "reason": "missing_analog_training_data"}
    if len(y_train) < min(_ANALOG_K_VALUES):
        return {"status": "missing", "reason": "insufficient_analog_training_data"}

    x_train = np.asarray(x_train, dtype=float)
    y_train = np.asarray(y_train, dtype=float)
    x_raw = _feature_vector(row)
    feature_indices = _analog_feature_indices()
    if not feature_indices:
        return {"status": "missing", "reason": "missing_analog_features"}

    target_side = _feature_side_label(x_raw)
    side_labels = np.asarray([_feature_side_label(vector) for vector in x_train])
    same_side_mask = side_labels == target_side
    side_count = int(np.sum(same_side_mask))
    if side_count >= min(_ANALOG_K_VALUES):
        eligible_mask = same_side_mask
        side_scope = target_side
    else:
        eligible_mask = np.ones(len(y_train), dtype=bool)
        side_scope = "all"

    x_pool = x_train[eligible_mask][:, feature_indices]
    y_pool = y_train[eligible_mask]
    if len(y_pool) < min(_ANALOG_K_VALUES):
        return {
            "status": "missing",
            "reason": "insufficient_analog_pool",
            "side_scope": side_scope,
            "same_side_count": side_count,
            "pool_count": int(len(y_pool)),
        }

    target = x_raw[feature_indices]
    median = np.nanmedian(x_pool, axis=0)
    q75 = np.nanpercentile(x_pool, 75, axis=0)
    q25 = np.nanpercentile(x_pool, 25, axis=0)
    scale = np.where((q75 - q25) < _EPS, np.nanstd(x_pool, axis=0), q75 - q25)
    scale = np.where(scale < _EPS, 1.0, scale)

    x_scaled = np.clip((np.nan_to_num(x_pool) - median) / scale, -_MODEL_FEATURE_CLIP, _MODEL_FEATURE_CLIP)
    target_scaled = np.clip((np.nan_to_num(target) - median) / scale, -_MODEL_FEATURE_CLIP, _MODEL_FEATURE_CLIP)
    distance = np.sqrt(np.mean((x_scaled - target_scaled) ** 2, axis=1))
    order = np.argsort(distance)
    desired_label = 1.0 if direction == "continuation" else 0.0

    best: dict | None = None
    for k_value in _ANALOG_K_VALUES:
        k = min(k_value, len(order))
        if k < min(_ANALOG_K_VALUES):
            continue
        nearest = order[:k]
        success_count = int(np.sum(y_pool[nearest] == desired_label))
        precision = success_count / k
        posterior = (success_count + 1.0) / (k + 2.0)
        lower_bound = _wilson_lower_bound(success_count, k)
        avg_distance = float(np.mean(distance[nearest]))
        candidate = {
            "status": "ready",
            "direction": direction,
            "side_scope": side_scope,
            "same_side_count": side_count,
            "pool_count": int(len(y_pool)),
            "neighbor_count": int(k),
            "success_count": success_count,
            "precision": float(precision),
            "posterior_probability": float(posterior),
            "lower_bound": float(lower_bound),
            "avg_distance": avg_distance,
        }
        if best is None or (
            candidate["posterior_probability"],
            candidate["precision"],
            -candidate["avg_distance"],
        ) > (
            best["posterior_probability"],
            best["precision"],
            -best["avg_distance"],
        ):
            best = candidate

    return best or {"status": "missing", "reason": "insufficient_analog_neighbors"}


def _fit_supervised_model(
    feature_df: pd.DataFrame,
    *,
    target_idx: int,
    horizon: int,
) -> tuple[dict | None, str | None]:
    x_train, y_train, indices = _build_training_matrix(feature_df, target_idx=target_idx, horizon=horizon)
    sample_count = int(len(y_train))
    positive_count = int(np.sum(y_train == 1.0))
    negative_count = int(np.sum(y_train == 0.0))

    if sample_count < _MIN_TRAINING_SAMPLES:
        return {
            "sample_count": sample_count,
            "positive_count": positive_count,
            "negative_count": negative_count,
        }, "insufficient_training_data"
    if positive_count == 0 or negative_count == 0:
        return {
            "sample_count": sample_count,
            "positive_count": positive_count,
            "negative_count": negative_count,
        }, "insufficient_class_balance"

    candidates: list[dict] = []
    for spec in _candidate_specs():
        validation = _validate_candidate(x_train, y_train, indices, horizon=horizon, spec=spec)
        if validation is None:
            continue
        validation_rows = validation.get("predictions", [])
        continuation_zone = _direction_policy_from_rows(
            validation_rows,
            direction="continuation",
            precision_target=_CONTINUATION_PRECISION_THRESHOLD,
            min_calls=_MIN_CONTINUATION_VALIDATION_CALLS,
        )
        reversal_zone = _direction_policy_from_rows(
            validation_rows,
            direction="reversal",
            precision_target=_REVERSAL_PRECISION_THRESHOLD,
            min_calls=_MIN_REVERSAL_VALIDATION_CALLS,
        )
        policy_edge = max(
            (
                _safe_num(continuation_zone.get("precision"), 0.0) - _CONTINUATION_PRECISION_THRESHOLD
                if continuation_zone
                else 0.0
            ),
            (
                _safe_num(reversal_zone.get("precision"), 0.0) - _REVERSAL_PRECISION_THRESHOLD
                if reversal_zone
                else 0.0
            ),
        )
        if validation["accuracy"] < _MIN_VALIDATION_ACCURACY and not (continuation_zone or reversal_zone):
            continue
        candidate = _fit_candidate(x_train, y_train, indices, spec)
        if candidate is None:
            continue
        edge = max(0.001, validation["accuracy"] - 0.5, policy_edge)
        candidate["validation"] = validation
        candidate["weight"] = edge * math.sqrt(validation["validation_count"]) / max(validation["brier"], 0.05)
        candidates.append(candidate)

    if not candidates:
        return {
            "sample_count": sample_count,
            "positive_count": positive_count,
            "negative_count": negative_count,
        }, "weak_validation_edge"

    return {
        "kind": "ensemble",
        "sample_count": sample_count,
        "positive_count": positive_count,
        "negative_count": negative_count,
        "candidates": candidates,
        "precision_policy": _derive_precision_policy(candidates),
        "x_train": x_train,
        "y_train": y_train,
        "indices": indices,
    }, None


def _predict_with_model(row: pd.Series, horizon: int, model: dict) -> dict:
    x_raw = _feature_vector(row)
    candidates = model.get("candidates", [])
    weighted_probability = 0.0
    total_weight = 0.0
    for candidate in candidates:
        weight = _safe_num(candidate.get("weight"), 0.0)
        if weight <= 0:
            continue
        weighted_probability += weight * _predict_candidate_probability(x_raw, candidate)
        total_weight += weight

    raw_probability = _clamp(weighted_probability / total_weight if total_weight > _EPS else 0.5, 0.01, 0.99)
    raw_predicted_direction = "continuation" if raw_probability >= 0.5 else "reversal"
    precision_policy = model.get("precision_policy", {}) or {}
    row_side = _training_side_for_row(row)
    side_policy_key = "upper" if row_side == "Upper" else "lower" if row_side == "Lower" else "overall"
    side_policy = precision_policy.get(side_policy_key) if isinstance(precision_policy.get(side_policy_key), dict) else None
    overall_policy = precision_policy.get("overall") if isinstance(precision_policy.get("overall"), dict) else precision_policy
    active_policy = side_policy or overall_policy or {}
    continuation_policy = active_policy.get("continuation")
    reversal_policy = active_policy.get("reversal")
    validation_accuracy = (
        sum(_safe_num(c.get("weight")) * _safe_num(c.get("validation", {}).get("accuracy")) for c in candidates)
        / total_weight
        if total_weight > _EPS
        else None
    )
    continuation_precision_values = [
        (_safe_num(c.get("weight")), c.get("validation", {}).get("continuation_precision"))
        for c in candidates
        if c.get("validation", {}).get("continuation_precision") is not None
    ]
    reversal_precision_values = [
        (_safe_num(c.get("weight")), c.get("validation", {}).get("reversal_precision"))
        for c in candidates
        if c.get("validation", {}).get("reversal_precision") is not None
    ]
    continuation_validation_precision = (
        sum(weight * _safe_num(value) for weight, value in continuation_precision_values)
        / sum(weight for weight, _ in continuation_precision_values)
        if continuation_precision_values and sum(weight for weight, _ in continuation_precision_values) > _EPS
        else None
    )
    reversal_validation_precision = (
        sum(weight * _safe_num(value) for weight, value in reversal_precision_values)
        / sum(weight for weight, _ in reversal_precision_values)
        if reversal_precision_values and sum(weight for weight, _ in reversal_precision_values) > _EPS
        else None
    )
    continuation_validation_count_values = [
        (_safe_num(c.get("weight")), _safe_num(c.get("validation", {}).get("continuation_prediction_count")))
        for c in candidates
    ]
    reversal_validation_count_values = [
        (_safe_num(c.get("weight")), _safe_num(c.get("validation", {}).get("reversal_prediction_count")))
        for c in candidates
    ]
    continuation_validation_count = (
        sum(weight * value for weight, value in continuation_validation_count_values)
        / sum(weight for weight, _ in continuation_validation_count_values)
        if continuation_validation_count_values
        and sum(weight for weight, _ in continuation_validation_count_values) > _EPS
        else 0.0
    )
    reversal_validation_count = (
        sum(weight * value for weight, value in reversal_validation_count_values)
        / sum(weight for weight, _ in reversal_validation_count_values)
        if reversal_validation_count_values
        and sum(weight for weight, _ in reversal_validation_count_values) > _EPS
        else 0.0
    )

    policy_matches: list[tuple[str, dict]] = []
    if isinstance(continuation_policy, dict) and raw_probability >= _safe_num(continuation_policy.get("cutoff"), 1.0):
        policy_matches.append(("continuation", continuation_policy))
    if isinstance(reversal_policy, dict) and raw_probability <= _safe_num(reversal_policy.get("cutoff"), 0.0):
        policy_matches.append(("reversal", reversal_policy))

    selected_direction: str | None = None
    selected_policy: dict | None = None
    if policy_matches:
        selected_direction, selected_policy = sorted(
            policy_matches,
            key=lambda item: (
                _safe_num(item[1].get("precision"), 0.0),
                _safe_num(item[1].get("call_count"), 0.0),
            ),
            reverse=True,
        )[0]

    if isinstance(continuation_policy, dict):
        continuation_validation_precision = _safe_num(
            continuation_policy.get("precision"),
            _safe_num(continuation_validation_precision, 0.0),
        )
        continuation_validation_count = _safe_num(
            continuation_policy.get("call_count"),
            continuation_validation_count,
        )
    if isinstance(reversal_policy, dict):
        reversal_validation_precision = _safe_num(
            reversal_policy.get("precision"),
            _safe_num(reversal_validation_precision, 0.0),
        )
        reversal_validation_count = _safe_num(
            reversal_policy.get("call_count"),
            reversal_validation_count,
        )

    balance = min(model["positive_count"], model["negative_count"]) / max(model["positive_count"], model["negative_count"])
    sample_strength = _clamp((model["sample_count"] - _MIN_TRAINING_SAMPLES) / 16.0, 0.0, 1.0)
    validation_strength = _clamp(((_safe_num(validation_accuracy, 0.5) - 0.45) / 0.30), 0.0, 1.0)
    policy_strength = _safe_num(selected_policy.get("precision"), 0.5) if selected_policy else 0.5
    shrink = balance * sample_strength * validation_strength * _clamp((policy_strength - 0.5) / 0.35, 0.15, 1.0)
    continuation_probability = raw_probability
    reversal_probability = 1.0 - continuation_probability

    if selected_direction == "continuation" and selected_policy is not None:
        continuation_probability = _clamp(
            max(continuation_probability, _safe_num(selected_policy.get("precision"), continuation_probability)),
            0.01,
            0.99,
        )
        reversal_probability = 1.0 - continuation_probability
    elif selected_direction == "reversal" and selected_policy is not None:
        reversal_probability = _clamp(
            max(reversal_probability, _safe_num(selected_policy.get("precision"), reversal_probability)),
            0.01,
            0.99,
        )
        continuation_probability = 1.0 - reversal_probability

    max_probability = max(continuation_probability, reversal_probability)
    confidence_score = int(round(max_probability * 100.0))
    validation_precision = _safe_num(selected_policy.get("precision"), np.nan) if selected_policy else np.nan
    if not np.isfinite(validation_precision):
        precision_key = (
            "continuation_precision"
            if raw_predicted_direction == "continuation"
            else "reversal_precision"
        )
        validation_precision_values = [
            (_safe_num(c.get("weight")), c.get("validation", {}).get(precision_key))
            for c in candidates
            if c.get("validation", {}).get(precision_key) is not None
        ]
        validation_precision = (
            sum(weight * _safe_num(value) for weight, value in validation_precision_values)
            / sum(weight for weight, _ in validation_precision_values)
            if validation_precision_values and sum(weight for weight, _ in validation_precision_values) > _EPS
            else np.nan
        )

    contribution_by_feature = {feature: 0.0 for feature in _MODEL_FEATURES}
    for candidate in candidates:
        model_weight = _safe_num(candidate.get("weight"), 0.0) / total_weight if total_weight > _EPS else 0.0
        x_scaled = np.clip((x_raw - candidate["mean"]) / candidate["scale"], -_MODEL_FEATURE_CLIP, _MODEL_FEATURE_CLIP)
        if candidate["kind"] == "logistic":
            signal = candidate["coef"] * x_scaled
        else:
            signal = (candidate["pos_centroid"] - candidate["neg_centroid"]) * x_scaled * 0.25
        for feature, contribution in zip(_MODEL_FEATURES, signal):
            contribution_by_feature[feature] += float(model_weight * contribution * shrink)

    contributions = [
        {
            "horizon": f"{horizon}d",
            "feature": feature,
            "value": _value_for_payload(value),
            "impact": "continuation" if contribution >= 0 else "reversal",
            "contribution": round(contribution, 6),
        }
        for feature, value, contribution in zip(_MODEL_FEATURES, x_raw, [contribution_by_feature[f] for f in _MODEL_FEATURES])
    ]

    raw_confidence = max(raw_probability, 1.0 - raw_probability)
    decision_direction = selected_direction
    analog_override = False
    if decision_direction is None and raw_confidence >= _PREDICTION_THRESHOLD:
        decision_direction = raw_predicted_direction
        analog_override = True

    reversal_veto_reason = _reversal_veto_reason(row, horizon) if decision_direction == "reversal" else None
    analog_evidence = _analog_meta_label(row, model, decision_direction) if decision_direction else None
    analog_reason: str | None = None
    if decision_direction == "reversal" and analog_evidence is not None:
        if analog_evidence.get("status") != "ready":
            analog_reason = analog_evidence.get("reason") or "insufficient_analog_evidence"
        elif (
            _safe_num(analog_evidence.get("precision"), 0.0) < _ANALOG_MIN_REVERSE_PRECISION
            or _safe_num(analog_evidence.get("posterior_probability"), 0.0) < _ANALOG_MIN_REVERSE_POSTERIOR
        ):
            analog_reason = "weak_reverse_analog_evidence"
    elif decision_direction == "continuation" and analog_evidence is not None:
        if analog_evidence.get("status") != "ready":
            analog_reason = analog_evidence.get("reason") or "insufficient_analog_evidence"
        elif (
            _safe_num(analog_evidence.get("precision"), 0.0) < _ANALOG_MIN_CONTINUE_PRECISION
            or _safe_num(analog_evidence.get("posterior_probability"), 0.0) < _ANALOG_MIN_CONTINUE_POSTERIOR
        ):
            analog_reason = "weak_continue_analog_evidence"

    if analog_evidence and analog_evidence.get("status") == "ready":
        analog_probability = _safe_num(analog_evidence.get("posterior_probability"), max_probability)
        if decision_direction == "continuation":
            continuation_probability = _clamp(
                max(continuation_probability, analog_probability)
                if analog_override
                else min(continuation_probability, analog_probability),
                0.01,
                0.99,
            )
            reversal_probability = 1.0 - continuation_probability
        elif decision_direction == "reversal":
            reversal_probability = _clamp(
                max(reversal_probability, analog_probability)
                if analog_override
                else min(reversal_probability, analog_probability),
                0.01,
                0.99,
            )
            continuation_probability = 1.0 - reversal_probability
        max_probability = max(continuation_probability, reversal_probability)
        confidence_score = int(round(max_probability * 100.0))

    if decision_direction is None:
        status = "no_prediction"
        predicted_direction = None
        no_prediction_reason = "low_confidence" if raw_confidence < _PREDICTION_THRESHOLD else "no_validated_precision_zone"
    elif reversal_veto_reason is not None:
        status = "no_prediction"
        predicted_direction = None
        no_prediction_reason = reversal_veto_reason
    elif analog_reason is not None:
        status = "no_prediction"
        predicted_direction = None
        no_prediction_reason = analog_reason
    elif decision_direction == "reversal" and reversal_probability < _REVERSAL_DEPLOYMENT_THRESHOLD:
        status = "no_prediction"
        predicted_direction = None
        no_prediction_reason = "weak_reverse_deployment_edge"
    elif decision_direction == "continuation" and continuation_probability < _CONTINUATION_DEPLOYMENT_THRESHOLD:
        status = "no_prediction"
        predicted_direction = None
        no_prediction_reason = "weak_continue_deployment_edge"
    elif max_probability < _PREDICTION_THRESHOLD:
        status = "no_prediction"
        predicted_direction = None
        no_prediction_reason = "low_confidence"
    else:
        status = "prediction"
        predicted_direction = decision_direction
        no_prediction_reason = None

    return {
        "status": status,
        "predicted_direction": predicted_direction,
        "continuation_probability": round(continuation_probability, 6),
        "reversal_probability": round(reversal_probability, 6),
        "continuation_confidence_score": int(round(continuation_probability * 100.0)),
        "reversal_confidence_score": int(round(reversal_probability * 100.0)),
        "continuation_validation_precision": (
            round(continuation_validation_precision, 6)
            if continuation_validation_precision is not None
            else None
        ),
        "reversal_validation_precision": (
            round(reversal_validation_precision, 6)
            if reversal_validation_precision is not None
            else None
        ),
        "continuation_validation_count": round(continuation_validation_count, 3),
        "reversal_validation_count": round(reversal_validation_count, 3),
        "validation_policy": {
            "validation_count": int(_safe_num(precision_policy.get("validation_count"), 0)),
            "active_side": side_policy_key,
            "active_validation_count": int(_safe_num(active_policy.get("validation_count"), 0)),
            "continuation": deepcopy(continuation_policy) if isinstance(continuation_policy, dict) else None,
            "reversal": deepcopy(reversal_policy) if isinstance(reversal_policy, dict) else None,
            "selected": deepcopy(selected_policy) if selected_policy is not None else None,
        },
        "confidence_score": confidence_score,
        "threshold": _PREDICTION_THRESHOLD,
        "no_prediction_reason": no_prediction_reason,
        "reversal_veto_reason": reversal_veto_reason,
        "analog_evidence": deepcopy(analog_evidence) if analog_evidence is not None else None,
        "analog_override": analog_override,
        "deployment_quality_gate": None,
        "blocked_prediction": None,
        "contributions": sorted(
            contributions,
            key=lambda item: abs(_safe_num(item.get("contribution"))),
            reverse=True,
        )[:10],
        "model": _model_metadata(
            model["sample_count"],
            model["positive_count"],
            model["negative_count"],
            validation_accuracy=validation_accuracy,
            validation_precision=float(validation_precision) if np.isfinite(validation_precision) else None,
            candidate_count=len(candidates),
        ),
    }


def _evaluate_horizon_from_context(feature_df: pd.DataFrame, row_index: int, horizon: int) -> dict:
    return _evaluate_horizon_with_adaptive_analogs(feature_df, row_index, horizon)


def evaluate_row_decision(
    row: pd.Series,
    *,
    feature_df: pd.DataFrame | None = None,
    row_index: int | None = None,
    force_prediction: bool = False,
) -> dict:
    touched_side = row.get("touched_side")
    touched = touched_side in ("Upper", "Lower")
    event_risk_blocked = _safe_bool(row.get("event_risk_blocked"))

    if event_risk_blocked:
        horizons = {f"{h}d": _no_prediction_horizon(h, "event_risk") for h in _HORIZONS}
        return {
            "touched_side": touched_side,
            "setup_type": _setup_type(touched_side),
            "event_risk_blocked": True,
            "horizons": horizons,
            "top_reasons": [],
        }

    if not touched and not force_prediction:
        horizons = {f"{h}d": _no_prediction_horizon(h, "no_bollinger_touch") for h in _HORIZONS}
        return {
            "touched_side": touched_side,
            "setup_type": "no_band_setup",
            "event_risk_blocked": False,
            "horizons": horizons,
            "top_reasons": [],
        }

    if feature_df is None or row_index is None:
        horizons = {f"{h}d": _no_prediction_horizon(h, "insufficient_training_data") for h in _HORIZONS}
    else:
        horizons = {f"{h}d": _evaluate_horizon_from_context(feature_df, row_index, h) for h in _HORIZONS}
    all_components = [
        component
        for horizon_payload in horizons.values()
        for component in horizon_payload.get("contributions", [])
    ]
    top_reasons = sorted(
        all_components,
        key=lambda item: abs(_safe_num(item.get("contribution"))),
        reverse=True,
    )[:8]

    return {
        "touched_side": touched_side,
        "setup_type": _setup_type(touched_side),
        "event_risk_blocked": False,
        "horizons": horizons,
        "top_reasons": top_reasons,
    }


def _build_decisions_by_index(feature_df: pd.DataFrame) -> dict[int, dict]:
    decisions: dict[int, dict] = {}
    for idx in range(len(feature_df)):
        row = feature_df.iloc[idx]
        if _safe_bool(row.get("event_risk_blocked")):
            continue
        if row.get("touched_side") not in ("Upper", "Lower"):
            continue
        decisions[idx] = evaluate_row_decision(row, feature_df=feature_df, row_index=idx)
    return decisions


def _empty_backtest_result(feature_df: pd.DataFrame, horizon: int, period_start: pd.Timestamp | None = None) -> dict:
    first = feature_df.iloc[0].get("date") if not feature_df.empty else None
    last = feature_df.iloc[-1].get("date") if not feature_df.empty else None
    return {
        "horizon_days": horizon,
        "period_start": _to_date_string(period_start if period_start is not None else first),
        "period_end": _to_date_string(last),
        "eligible_touch_count": 0,
        "prediction_count": 0,
        "sample_count": 0,
        "no_prediction_count": 0,
        "coverage": None,
        "correct_count": 0,
        "accuracy": None,
        "continuation_call_count": 0,
        "continuation_correct_count": 0,
        "continuation_accuracy": None,
        "reversal_call_count": 0,
        "reversal_correct_count": 0,
        "reversal_accuracy": None,
        "missed_reversal_count": 0,
        "flat_count": 0,
        "incomplete_future_count": 0,
        "signal_tier_counts": {},
        "predictions": [],
        "recent_predictions": [],
    }


def _run_horizon_backtest(
    feature_df: pd.DataFrame,
    *,
    horizon: int,
    decisions_by_index: dict[int, dict] | None = None,
) -> dict:
    if feature_df.empty:
        return _empty_backtest_result(feature_df, horizon)

    last_date = pd.Timestamp(feature_df.iloc[-1].get("date")).normalize()
    period_start = max(
        pd.Timestamp(feature_df.iloc[0].get("date")).normalize(),
        last_date - pd.DateOffset(days=_BACKTEST_LOOKBACK_DAYS),
    )

    eligible_touch_count = 0
    prediction_count = 0
    no_prediction_count = 0
    correct_count = 0
    continuation_call_count = 0
    continuation_correct_count = 0
    reversal_call_count = 0
    reversal_correct_count = 0
    missed_reversal_count = 0
    flat_count = 0
    incomplete_future_count = 0
    signal_tier_counts: dict[str, int] = {}
    predictions: list[dict] = []

    horizon_key = f"{horizon}d"
    for idx in range(len(feature_df)):
        row = feature_df.iloc[idx]
        signal_date = pd.Timestamp(row.get("date")).normalize()
        if signal_date < period_start:
            continue
        if _safe_bool(row.get("event_risk_blocked")):
            continue

        touched_side = row.get("touched_side")
        if touched_side not in ("Upper", "Lower"):
            continue

        if idx + horizon >= len(feature_df):
            incomplete_future_count += 1
            continue

        signal_close = _safe_num(row.get("close"), np.nan)
        outcome_row = feature_df.iloc[idx + horizon]
        outcome_close = _safe_num(outcome_row.get("close"), np.nan)
        if not np.isfinite(signal_close) or not np.isfinite(outcome_close):
            incomplete_future_count += 1
            continue

        eligible_touch_count += 1
        continuation_hurdle = _continuation_hurdle_for_row(row)
        actual_direction = _actual_direction(touched_side, signal_close, outcome_close, continuation_hurdle)
        if actual_direction == "flat" or (
            continuation_hurdle > _EPS and abs(outcome_close - signal_close) <= continuation_hurdle
        ):
            flat_count += 1

        if decisions_by_index is not None:
            if idx not in decisions_by_index:
                decisions_by_index[idx] = evaluate_row_decision(row, feature_df=feature_df, row_index=idx)
            decision = decisions_by_index[idx]
        else:
            decision = evaluate_row_decision(row, feature_df=feature_df, row_index=idx)
        horizon_decision = decision.get("horizons", {}).get(horizon_key) or _no_prediction_horizon(
            horizon,
            "low_confidence",
        )

        if horizon_decision.get("status") != "prediction":
            no_prediction_count += 1
            if actual_direction == "reversal":
                missed_reversal_count += 1
            continue

        predicted_direction = horizon_decision.get("predicted_direction")
        is_correct = predicted_direction == actual_direction and actual_direction != "flat"

        prediction_count += 1
        if predicted_direction == "continuation":
            continuation_call_count += 1
            if is_correct:
                continuation_correct_count += 1
        elif predicted_direction == "reversal":
            reversal_call_count += 1
            if is_correct:
                reversal_correct_count += 1
        if actual_direction == "reversal" and predicted_direction != "reversal":
            missed_reversal_count += 1
        if is_correct:
            correct_count += 1
        signal_tier = (horizon_decision.get("playbook") or {}).get("tier") or (
            (horizon_decision.get("playbook") or {}).get("profile") or {}
        ).get("tier")
        if signal_tier:
            signal_tier_counts[str(signal_tier)] = signal_tier_counts.get(str(signal_tier), 0) + 1

        predictions.append(
            {
                "signal_date": _to_date_string(row.get("date")),
                "outcome_date": _to_date_string(outcome_row.get("date")),
                "horizon_days": horizon,
                "touched_side": touched_side,
                "predicted_direction": predicted_direction,
                "actual_direction": actual_direction,
                "signal_close": round(signal_close, 6),
                "outcome_close": round(outcome_close, 6),
                "continuation_hurdle": round(continuation_hurdle, 6),
                "is_correct": bool(is_correct),
                "continuation_probability": horizon_decision.get("continuation_probability"),
                "reversal_probability": horizon_decision.get("reversal_probability"),
                "confidence_score": horizon_decision.get("confidence_score"),
                "signal_model": (horizon_decision.get("playbook") or {}).get("name"),
                "signal_model_id": (horizon_decision.get("playbook") or {}).get("id"),
                "signal_precision": (horizon_decision.get("playbook") or {}).get("precision"),
                "signal_tier": signal_tier,
            }
        )

    if eligible_touch_count == 0:
        return _empty_backtest_result(feature_df, horizon, period_start=period_start)

    coverage = prediction_count / eligible_touch_count
    accuracy = correct_count / prediction_count if prediction_count > 0 else None
    continuation_accuracy = (
        continuation_correct_count / continuation_call_count
        if continuation_call_count > 0
        else None
    )
    reversal_accuracy = reversal_correct_count / reversal_call_count if reversal_call_count > 0 else None

    return {
        "horizon_days": horizon,
        "period_start": _to_date_string(period_start),
        "period_end": _to_date_string(last_date),
        "eligible_touch_count": eligible_touch_count,
        "prediction_count": prediction_count,
        "sample_count": prediction_count,
        "no_prediction_count": no_prediction_count,
        "coverage": round(coverage, 6),
        "correct_count": correct_count,
        "accuracy": round(accuracy, 6) if accuracy is not None else None,
        "continuation_call_count": continuation_call_count,
        "continuation_correct_count": continuation_correct_count,
        "continuation_accuracy": round(continuation_accuracy, 6) if continuation_accuracy is not None else None,
        "reversal_call_count": reversal_call_count,
        "reversal_correct_count": reversal_correct_count,
        "reversal_accuracy": round(reversal_accuracy, 6) if reversal_accuracy is not None else None,
        "missed_reversal_count": missed_reversal_count,
        "flat_count": flat_count,
        "incomplete_future_count": incomplete_future_count,
        "signal_tier_counts": signal_tier_counts,
        "predictions": predictions,
        "recent_predictions": list(reversed(predictions[-20:])),
    }


def run_decision_backtest(
    feature_df: pd.DataFrame,
    decisions_by_index: dict[int, dict] | None = None,
) -> dict:
    shared_decisions = decisions_by_index if decisions_by_index is not None else {}
    return {
        f"{h}d": _run_horizon_backtest(
            feature_df,
            horizon=h,
            decisions_by_index=shared_decisions,
        )
        for h in _HORIZONS
    }


def _deployment_quality_gate(backtest: dict) -> dict:
    prediction_count = int(_safe_num(backtest.get("prediction_count"), 0))
    failures: list[str] = []

    if prediction_count == 0:
        return {
            "status": "idle",
            "deployment_enabled": True,
            "failures": [],
            "min_prediction_count": _DEPLOY_MIN_BACKTEST_CALLS,
            "min_accuracy": _DEPLOY_MIN_BACKTEST_ACCURACY,
            "min_reverse_accuracy": _DEPLOY_MIN_BACKTEST_REVERSE_ACCURACY,
            "min_continue_accuracy": _DEPLOY_MIN_BACKTEST_CONTINUE_ACCURACY,
            "raw_prediction_count": 0,
            "raw_accuracy": None,
            "raw_reverse_accuracy": None,
            "raw_continue_accuracy": None,
        }

    accuracy = backtest.get("accuracy")
    reversal_accuracy = backtest.get("reversal_accuracy")
    continuation_accuracy = backtest.get("continuation_accuracy")
    reversal_call_count = int(_safe_num(backtest.get("reversal_call_count"), 0))
    continuation_call_count = int(_safe_num(backtest.get("continuation_call_count"), 0))

    if prediction_count < _DEPLOY_MIN_BACKTEST_CALLS:
        failures.append("insufficient_deployed_sample")
    if accuracy is None or _safe_num(accuracy, 0.0) < _DEPLOY_MIN_BACKTEST_ACCURACY:
        failures.append("weak_deployed_accuracy")
    if reversal_call_count > 0 and (
        reversal_accuracy is None or _safe_num(reversal_accuracy, 0.0) < _DEPLOY_MIN_BACKTEST_REVERSE_ACCURACY
    ):
        failures.append("weak_reverse_accuracy")
    if continuation_call_count > 0 and (
        continuation_accuracy is None or _safe_num(continuation_accuracy, 0.0) < _DEPLOY_MIN_BACKTEST_CONTINUE_ACCURACY
    ):
        failures.append("weak_continue_accuracy")

    return {
        "status": "quarantined" if failures else "passed",
        "deployment_enabled": not failures,
        "failures": failures,
        "min_prediction_count": _DEPLOY_MIN_BACKTEST_CALLS,
        "min_accuracy": _DEPLOY_MIN_BACKTEST_ACCURACY,
        "min_reverse_accuracy": _DEPLOY_MIN_BACKTEST_REVERSE_ACCURACY,
        "min_continue_accuracy": _DEPLOY_MIN_BACKTEST_CONTINUE_ACCURACY,
        "raw_prediction_count": prediction_count,
        "raw_accuracy": accuracy,
        "raw_reverse_accuracy": reversal_accuracy,
        "raw_continue_accuracy": continuation_accuracy,
    }


def _horizon_from_key(horizon_key: str) -> int:
    try:
        return int(str(horizon_key).lower().replace("d", ""))
    except (TypeError, ValueError):
        return 0


def _quality_gate_blocked_horizon(horizon_key: str, original: dict, gate: dict) -> dict:
    horizon = _horizon_from_key(horizon_key)
    model_meta = original.get("model", {}) if isinstance(original, dict) else {}
    blocked = _no_prediction_horizon(
        horizon,
        "deployment_quality_gate_failed",
        training_sample_count=int(_safe_num(model_meta.get("training_sample_count"), 0)),
        positive_count=int(_safe_num(model_meta.get("continuation_training_count"), 0)),
        negative_count=int(_safe_num(model_meta.get("reversal_training_count"), 0)),
    )
    blocked["deployment_quality_gate"] = deepcopy(gate)
    blocked["blocked_prediction"] = {
        "status": original.get("status"),
        "predicted_direction": original.get("predicted_direction"),
        "continuation_probability": original.get("continuation_probability"),
        "reversal_probability": original.get("reversal_probability"),
        "confidence_score": original.get("confidence_score"),
        "no_prediction_reason": original.get("no_prediction_reason"),
    }
    blocked["model"] = deepcopy(model_meta) if model_meta else blocked["model"]
    return blocked


def _direction_quality_gate(backtest: dict, direction: str) -> dict:
    if direction == "continuation":
        call_count = int(_safe_num(backtest.get("continuation_call_count"), 0))
        accuracy = backtest.get("continuation_accuracy")
        min_accuracy = _DEPLOY_MIN_BACKTEST_CONTINUE_ACCURACY
    else:
        call_count = int(_safe_num(backtest.get("reversal_call_count"), 0))
        accuracy = backtest.get("reversal_accuracy")
        min_accuracy = _DEPLOY_MIN_BACKTEST_REVERSE_ACCURACY

    failures: list[str] = []
    if call_count < _DEPLOY_MIN_BACKTEST_CALLS:
        failures.append("insufficient_direction_sample")
    if accuracy is None or _safe_num(accuracy, 0.0) < min_accuracy:
        failures.append(f"weak_{direction}_accuracy")

    return {
        "status": "quarantined" if failures else "passed",
        "deployment_enabled": not failures,
        "direction": direction,
        "failures": failures,
        "min_prediction_count": _DEPLOY_MIN_BACKTEST_CALLS,
        "min_accuracy": min_accuracy,
        "raw_prediction_count": call_count,
        "raw_accuracy": accuracy,
    }


def _direction_quality_gates(backtest_by_horizon: dict) -> dict[str, dict]:
    gates: dict[str, dict] = {}
    for horizon_key, result in backtest_by_horizon.items():
        if not isinstance(result, dict):
            continue
        continuation_gate = _direction_quality_gate(result, "continuation")
        reversal_gate = _direction_quality_gate(result, "reversal")
        signal_gates = _signal_quality_gates_for_horizon(result, continuation_gate, reversal_gate)
        _apply_max_safe_coverage_to_signal_gates(result, signal_gates)
        disabled_signals = [
            gate_key
            for gate_key, gate in signal_gates.items()
            if not gate.get("deployment_enabled", False)
        ]
        failures = continuation_gate["failures"] + reversal_gate["failures"]
        gates[horizon_key] = {
            "status": "partial" if disabled_signals else "passed",
            "deployment_enabled": True,
            "failures": failures,
            "continuation": continuation_gate,
            "reversal": reversal_gate,
            "signals": signal_gates,
        }
    return gates


def _signal_gate_key(direction: str | None, signal_id: str | None) -> str:
    return f"{direction or 'unknown'}:{signal_id or 'unknown'}"


def _prediction_signal_id_from_backtest_row(row: dict) -> str:
    return str(row.get("signal_model_id") or row.get("signal_tier") or "unknown")


def _prediction_signal_id_from_horizon(horizon_decision: dict) -> str:
    playbook = horizon_decision.get("playbook") or {}
    profile = playbook.get("profile") or {}
    return str(playbook.get("id") or profile.get("id") or playbook.get("tier") or "unknown")


def _signal_quality_gates_for_horizon(
    backtest: dict,
    continuation_gate: dict,
    reversal_gate: dict,
) -> dict[str, dict]:
    grouped: dict[str, dict] = {}
    for prediction in backtest.get("predictions", []):
        direction = prediction.get("predicted_direction")
        if direction not in ("continuation", "reversal"):
            continue
        signal_id = _prediction_signal_id_from_backtest_row(prediction)
        key = _signal_gate_key(direction, signal_id)
        bucket = grouped.setdefault(
            key,
            {
                "direction": direction,
                "signal_id": signal_id,
                "prediction_count": 0,
                "correct_count": 0,
            },
        )
        bucket["prediction_count"] += 1
        if prediction.get("is_correct"):
            bucket["correct_count"] += 1

    gates: dict[str, dict] = {}
    for key, bucket in grouped.items():
        direction = bucket["direction"]
        direction_gate = continuation_gate if direction == "continuation" else reversal_gate
        min_accuracy = (
            _DEPLOY_MIN_BACKTEST_CONTINUE_ACCURACY
            if direction == "continuation"
            else _DEPLOY_MIN_BACKTEST_REVERSE_ACCURACY
        )
        prediction_count = int(bucket["prediction_count"])
        correct_count = int(bucket["correct_count"])
        accuracy = correct_count / prediction_count if prediction_count else None
        failures: list[str] = []
        inherited_direction_pass = bool(direction_gate.get("deployment_enabled"))

        if prediction_count < _DEPLOY_MIN_SIGNAL_BACKTEST_CALLS and not inherited_direction_pass:
            failures.append("insufficient_signal_sample")
        if accuracy is None or _safe_num(accuracy, 0.0) < min_accuracy:
            failures.append(f"weak_{direction}_signal_accuracy")
        if inherited_direction_pass and prediction_count < _DEPLOY_MIN_SIGNAL_BACKTEST_CALLS:
            failures = [failure for failure in failures if failure != "insufficient_signal_sample"]

        gates[key] = {
            "status": "quarantined" if failures else "passed",
            "deployment_enabled": not failures,
            "direction": direction,
            "signal_id": bucket["signal_id"],
            "failures": failures,
            "min_prediction_count": _DEPLOY_MIN_SIGNAL_BACKTEST_CALLS,
            "min_accuracy": min_accuracy,
            "raw_prediction_count": prediction_count,
            "raw_correct_count": correct_count,
            "raw_accuracy": round(accuracy, 6) if accuracy is not None else None,
            "direction_gate_status": direction_gate.get("status"),
        }
    return gates


def _prediction_group_key(prediction: dict) -> str:
    return _signal_gate_key(
        prediction.get("predicted_direction"),
        _prediction_signal_id_from_backtest_row(prediction),
    )


def _prediction_book_metrics(predictions: list[dict]) -> dict:
    prediction_count = len(predictions)
    correct_count = sum(1 for item in predictions if item.get("is_correct"))
    continuation = [item for item in predictions if item.get("predicted_direction") == "continuation"]
    reversal = [item for item in predictions if item.get("predicted_direction") == "reversal"]
    continuation_correct = sum(1 for item in continuation if item.get("is_correct"))
    reversal_correct = sum(1 for item in reversal if item.get("is_correct"))
    return {
        "prediction_count": prediction_count,
        "correct_count": correct_count,
        "accuracy": correct_count / prediction_count if prediction_count else None,
        "continuation_call_count": len(continuation),
        "continuation_accuracy": continuation_correct / len(continuation) if continuation else None,
        "reversal_call_count": len(reversal),
        "reversal_accuracy": reversal_correct / len(reversal) if reversal else None,
    }


def _prediction_book_passes_quality(metrics: dict) -> bool:
    if int(_safe_num(metrics.get("prediction_count"), 0)) <= 0:
        return False
    if _safe_num(metrics.get("accuracy"), 0.0) < _DEPLOY_MIN_BACKTEST_ACCURACY:
        return False
    if int(_safe_num(metrics.get("reversal_call_count"), 0)) > 0 and (
        metrics.get("reversal_accuracy") is None
        or _safe_num(metrics.get("reversal_accuracy"), 0.0) < _DEPLOY_MIN_BACKTEST_REVERSE_ACCURACY
    ):
        return False
    if int(_safe_num(metrics.get("continuation_call_count"), 0)) > 0 and (
        metrics.get("continuation_accuracy") is None
        or _safe_num(metrics.get("continuation_accuracy"), 0.0) < _DEPLOY_MIN_BACKTEST_CONTINUE_ACCURACY
    ):
        return False
    return True


def _prediction_book_preserves_accuracy(current_metrics: dict, trial_metrics: dict) -> bool:
    for key in ("accuracy", "continuation_accuracy", "reversal_accuracy"):
        current = current_metrics.get(key)
        if current is None:
            continue
        trial = trial_metrics.get(key)
        if trial is None or _safe_num(trial, 0.0) + _EPS < _safe_num(current, 0.0):
            return False
    return True


def _prediction_row_key(row: dict) -> tuple:
    return (
        row.get("_repair_row_key")
        or row.get("signal_date")
        or row.get("date"),
        row.get("horizon_days"),
    )


def _candidate_rows(selected_rows: list[dict], candidate_set: list[dict]) -> list[dict]:
    rows: list[dict] = []
    seen: set[tuple] = set()
    for row in selected_rows + [
        item
        for candidate in candidate_set
        for item in candidate["rows"]
    ]:
        key = _prediction_row_key(row)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)
    return rows


def _candidate_selection_key(candidates: list[dict], metrics: dict) -> tuple:
    return (
        int(_safe_num(metrics.get("prediction_count"), 0)),
        _safe_num(metrics.get("accuracy"), -1.0),
        _safe_num(metrics.get("reversal_accuracy"), -1.0),
        _safe_num(metrics.get("continuation_accuracy"), -1.0),
        sum(1 for candidate in candidates if candidate["gate"].get("direction") == "reversal"),
    )


def _candidate_priority_key(candidate: dict) -> tuple:
    return (
        int(_safe_num(candidate["metrics"].get("prediction_count"), 0)),
        _safe_num(candidate["metrics"].get("accuracy"), 0.0),
        1 if candidate["gate"].get("direction") == "reversal" else 0,
    )


def _select_max_safe_coverage_candidates(selected_rows: list[dict], candidates: list[dict]) -> list[dict]:
    baseline_metrics = _prediction_book_metrics(selected_rows)

    def candidate_is_safe(candidate_set: list[dict]) -> tuple[bool, dict]:
        rows = _candidate_rows(selected_rows, candidate_set)
        metrics = _prediction_book_metrics(rows)
        return (
            _prediction_book_passes_quality(metrics)
            and _prediction_book_preserves_accuracy(baseline_metrics, metrics),
            metrics,
        )

    if len(candidates) <= _MAX_EXACT_COVERAGE_EXPANSION_CANDIDATES:
        best_candidates: list[dict] = []
        best_key = _candidate_selection_key([], baseline_metrics)
        for mask in range(1, 1 << len(candidates)):
            candidate_set = [
                candidate
                for index, candidate in enumerate(candidates)
                if mask & (1 << index)
            ]
            is_safe, metrics = candidate_is_safe(candidate_set)
            if not is_safe:
                continue
            key = _candidate_selection_key(candidate_set, metrics)
            if key > best_key:
                best_key = key
                best_candidates = candidate_set
        return best_candidates

    remaining = sorted(candidates, key=_candidate_priority_key, reverse=True)
    chosen: list[dict] = []
    while remaining:
        safe_candidates: list[tuple[tuple, dict]] = []
        for candidate in remaining:
            is_safe, metrics = candidate_is_safe(chosen + [candidate])
            if is_safe:
                safe_candidates.append((_candidate_selection_key(chosen + [candidate], metrics), candidate))
        if not safe_candidates:
            break
        _, next_candidate = max(safe_candidates, key=lambda item: item[0])
        chosen.append(next_candidate)
        remaining = [candidate for candidate in remaining if candidate is not next_candidate]
    return chosen


def _apply_max_safe_coverage_to_signal_gates(backtest: dict, signal_gates: dict[str, dict]) -> None:
    eligible_count = int(_safe_num(backtest.get("eligible_touch_count"), 0))
    if eligible_count <= 0:
        backtest["coverage_policy"] = _COVERAGE_POLICY_MAX_SAFE
        return

    predictions = list(backtest.get("predictions", []))
    if not predictions:
        backtest["coverage_policy"] = _COVERAGE_POLICY_MAX_SAFE
        backtest["max_safe_prediction_count"] = 0
        backtest["max_safe_coverage"] = 0.0
        return

    selected = [
        item
        for item in predictions
        if signal_gates.get(_prediction_group_key(item), {}).get("deployment_enabled", False)
    ]

    groups: dict[str, list[dict]] = {}
    for item in predictions:
        key = _prediction_group_key(item)
        if signal_gates.get(key, {}).get("deployment_enabled", False):
            continue
        groups.setdefault(key, []).append(item)

    candidates: list[dict] = []
    for key, rows in groups.items():
        metrics = _prediction_book_metrics(rows)
        group_accuracy = _safe_num(metrics.get("accuracy"), 0.0)
        if group_accuracy < _COVERAGE_EXPANSION_MIN_SIGNAL_ACCURACY:
            continue
        gate = signal_gates.get(key, {})
        candidates.append(
            {
                "key": key,
                "rows": rows,
                "metrics": metrics,
                "gate": gate,
                "accuracy": group_accuracy,
            }
        )

    selected_candidates = _select_max_safe_coverage_candidates(selected, candidates)
    selected_keys = [candidate["key"] for candidate in selected_candidates]
    final_rows = selected + [
        row
        for selected_candidate in selected_candidates
        for row in selected_candidate["rows"]
    ]
    final_metrics = _prediction_book_metrics(final_rows)

    for candidate in selected_candidates:
        gate = signal_gates[candidate["key"]]
        gate["status"] = "coverage_expansion"
        gate["deployment_enabled"] = True
        gate["coverage_expansion"] = True
        gate["coverage_policy"] = _COVERAGE_POLICY_MAX_SAFE
        gate["portfolio_accuracy_after_expansion"] = round(_safe_num(final_metrics.get("accuracy")), 6)
        gate["portfolio_reverse_accuracy_after_expansion"] = (
            round(_safe_num(final_metrics.get("reversal_accuracy")), 6)
            if final_metrics.get("reversal_accuracy") is not None
            else None
        )
        gate["portfolio_continue_accuracy_after_expansion"] = (
            round(_safe_num(final_metrics.get("continuation_accuracy")), 6)
            if final_metrics.get("continuation_accuracy") is not None
            else None
        )
        gate["failures"] = []

    backtest["coverage_policy"] = _COVERAGE_POLICY_MAX_SAFE
    backtest["max_safe_prediction_count"] = int(_safe_num(final_metrics.get("prediction_count"), 0))
    backtest["max_safe_coverage"] = round(len(final_rows) / eligible_count, 6)
    if selected_keys:
        backtest["coverage_expansion_signal_count"] = len(selected_keys)


def _coverage_repair_specs() -> list[tuple[str, tuple[str, ...], int]]:
    return [
        ("side_touch_cluster_trend", ("side", "touch_quality", "cluster", "trend"), 3),
        ("side_touch_trend", ("side", "touch_quality", "trend"), 3),
        ("side_cluster_trend", ("side", "cluster", "trend"), 3),
        ("side_reason_trend", ("side", "reason", "trend"), 3),
        ("side_touch_cluster", ("side", "touch_quality", "cluster"), 4),
        ("side_trend", ("side", "trend"), 4),
        ("side_cluster", ("side", "cluster"), 4),
        ("side_reason", ("side", "reason"), 4),
        ("side_only", ("side",), 8),
    ]


def _coverage_repair_attrs(row: pd.Series, horizon_decision: dict, horizon: int) -> dict[str, str]:
    attrs = _empirical_regime_attrs(row, horizon)
    attrs["reason"] = str(horizon_decision.get("no_prediction_reason") or "unknown")
    blocked = horizon_decision.get("blocked_prediction") or {}
    attrs["blocked_direction"] = str(blocked.get("predicted_direction") or "none")
    return attrs


def _coverage_repair_policy_id(scope: str, direction: str, fields: tuple[str, ...], attrs: dict[str, str]) -> str:
    pieces = [scope, direction] + [f"{field}_{attrs.get(field, 'na')}" for field in fields]
    text = "_".join(pieces).lower()
    return "coverage_repair_" + re.sub(r"[^a-z0-9]+", "_", text).strip("_")


def _coverage_repair_prediction_row(
    feature_df: pd.DataFrame,
    idx: int,
    horizon: int,
    direction: str,
    actual_direction: str,
    policy: dict,
) -> dict:
    row = feature_df.iloc[idx]
    return {
        "_repair_row_key": (int(idx), horizon),
        "_repair_policy": policy,
        "signal_date": _to_date_string(row.get("date")),
        "outcome_date": _to_date_string(feature_df.iloc[idx + horizon].get("date")),
        "horizon_days": horizon,
        "touched_side": row.get("touched_side"),
        "predicted_direction": direction,
        "actual_direction": actual_direction,
        "confidence_score": int(round(_safe_num(policy.get("precision"), 0.0) * 100.0)),
        "is_correct": direction == actual_direction,
        "signal_model": "Selective Coverage Repair",
        "signal_model_id": policy["id"],
        "signal_precision": policy["precision"],
        "signal_tier": "coverage_repair",
    }


def _coverage_repair_candidates(
    feature_df: pd.DataFrame,
    decisions_by_index: dict[int, dict],
    horizon: int,
) -> list[dict]:
    if feature_df.empty:
        return []

    horizon_key = f"{horizon}d"
    last_date = pd.Timestamp(feature_df.iloc[-1].get("date")).normalize()
    period_start = max(
        pd.Timestamp(feature_df.iloc[0].get("date")).normalize(),
        last_date - pd.DateOffset(days=_BACKTEST_LOOKBACK_DAYS),
    )
    groups: dict[tuple, list[dict]] = {}

    for idx, decision in decisions_by_index.items():
        if idx + horizon >= len(feature_df):
            continue
        row = feature_df.iloc[idx]
        signal_date = pd.Timestamp(row.get("date")).normalize()
        if signal_date < period_start:
            continue
        if _safe_bool(row.get("event_risk_blocked")):
            continue
        if row.get("touched_side") not in ("Upper", "Lower"):
            continue
        horizon_decision = decision.get("horizons", {}).get(horizon_key) or {}
        if horizon_decision.get("status") == "prediction":
            continue
        actual = _actual_direction_for_index(feature_df, idx, horizon)
        if actual not in ("continuation", "reversal"):
            continue
        attrs = _coverage_repair_attrs(row, horizon_decision, horizon)
        for scope, fields, min_matches in _coverage_repair_specs():
            key = (scope, fields, tuple((field, attrs.get(field)) for field in fields), min_matches)
            groups.setdefault(key, []).append(
                {
                    "idx": int(idx),
                    "actual_direction": actual,
                    "attrs": attrs,
                }
            )

    candidates: list[dict] = []
    for (scope, fields, field_values, min_matches), rows in groups.items():
        if len(rows) < max(min_matches, _COVERAGE_REPAIR_MIN_MATCHES):
            continue
        attrs = dict(field_values)
        for direction in ("reversal", "continuation"):
            correct_count = sum(1 for row in rows if row["actual_direction"] == direction)
            match_count = len(rows)
            precision = correct_count / match_count
            wilson = _wilson_lower_bound(correct_count, match_count)
            if (
                precision < _COVERAGE_REPAIR_MIN_PRECISION
                or wilson < _COVERAGE_REPAIR_MIN_WILSON
            ):
                continue
            policy = {
                "id": _coverage_repair_policy_id(scope, direction, fields, attrs),
                "name": "Selective Coverage Repair",
                "direction": direction,
                "tier": "coverage_repair",
                "scope": scope,
                "fields": list(fields),
                "attrs": attrs,
                "match_count": match_count,
                "correct_count": correct_count,
                "precision": round(precision, 6),
                "posterior_probability": round((correct_count + 1.0) / (match_count + 2.0), 6),
                "wilson_lower_bound": round(wilson, 6),
            }
            prediction_rows = [
                _coverage_repair_prediction_row(
                    feature_df,
                    row["idx"],
                    horizon,
                    direction,
                    row["actual_direction"],
                    policy,
                )
                for row in rows
            ]
            candidates.append(
                {
                    "key": policy["id"],
                    "rows": prediction_rows,
                    "metrics": _prediction_book_metrics(prediction_rows),
                    "gate": {"direction": direction},
                    "policy": policy,
                }
            )
    return candidates


def _coverage_repair_policy_matches(row: pd.Series, horizon_decision: dict, horizon: int, policy: dict) -> bool:
    attrs = _coverage_repair_attrs(row, horizon_decision, horizon)
    return all(str(attrs.get(field)) == str(policy.get("attrs", {}).get(field)) for field in policy.get("fields", []))


def _coverage_repair_horizon(
    feature_df: pd.DataFrame,
    idx: int,
    horizon: int,
    policy: dict,
) -> dict:
    direction = policy["direction"]
    precision = _clamp(_safe_num(policy.get("precision"), 0.5), 0.01, 0.99)
    posterior = _clamp(_safe_num(policy.get("posterior_probability"), precision), 0.01, 0.99)
    probability = max(precision, posterior)
    if direction == "continuation":
        continuation_probability = probability
        reversal_probability = 1.0 - probability
    else:
        reversal_probability = probability
        continuation_probability = 1.0 - probability

    training_count, continuation_count, reversal_count = _adaptive_training_summary(feature_df, idx, horizon)
    model = _model_metadata(
        training_count,
        continuation_count,
        reversal_count,
        validation_accuracy=precision,
        validation_precision=precision,
        candidate_count=1,
    )
    model["type"] = "selective_coverage_repair"
    return {
        "status": "prediction",
        "predicted_direction": direction,
        "continuation_probability": round(continuation_probability, 6),
        "reversal_probability": round(reversal_probability, 6),
        "continuation_confidence_score": int(round(continuation_probability * 100)),
        "reversal_confidence_score": int(round(reversal_probability * 100)),
        "continuation_validation_precision": precision if direction == "continuation" else None,
        "reversal_validation_precision": precision if direction == "reversal" else None,
        "continuation_validation_count": policy["match_count"] if direction == "continuation" else 0,
        "reversal_validation_count": policy["match_count"] if direction == "reversal" else 0,
        "validation_policy": {
            "coverage_policy": _COVERAGE_POLICY_MAX_SAFE,
            "min_precision": _COVERAGE_REPAIR_MIN_PRECISION,
            "min_wilson_lower_bound": _COVERAGE_REPAIR_MIN_WILSON,
            "scope": policy["scope"],
            "fields": policy["fields"],
            "attrs": policy["attrs"],
        },
        "confidence_score": int(round(max(continuation_probability, reversal_probability) * 100)),
        "threshold": _COVERAGE_REPAIR_MIN_PRECISION,
        "no_prediction_reason": None,
        "reversal_veto_reason": None,
        "analog_evidence": {
            "status": "ready",
            "direction": direction,
            "posterior_probability": policy["posterior_probability"],
            "neighbor_count": policy["match_count"],
            "precision": policy["precision"],
            "lower_bound": policy["wilson_lower_bound"],
        },
        "analog_override": False,
        "deployment_quality_gate": {
            "status": "coverage_repair",
            "deployment_enabled": True,
            "coverage_policy": _COVERAGE_POLICY_MAX_SAFE,
        },
        "blocked_prediction": None,
        "playbook": deepcopy(policy),
        "adaptive_candidates": [
            {
                "profile_id": policy["id"],
                "status": "ready",
                "direction": direction,
                "tier": "coverage_repair",
                "precision": policy["precision"],
                "validation_count": policy["match_count"],
                "confidence": round(max(continuation_probability, reversal_probability), 6),
                "blocked_reason": None,
            }
        ],
        "contributions": [
            {
                "horizon": f"{horizon}d",
                "feature": policy["name"],
                "value": policy["match_count"],
                "impact": direction,
                "contribution": policy["precision"],
            }
        ],
        "model": model,
    }


def _apply_coverage_repair_policies(
    feature_df: pd.DataFrame,
    decisions_by_index: dict[int, dict],
    policies_by_horizon: dict[str, list[dict]],
) -> None:
    for idx, decision in decisions_by_index.items():
        row = feature_df.iloc[idx]
        if row.get("touched_side") not in ("Upper", "Lower"):
            continue
        if _safe_bool(row.get("event_risk_blocked")):
            continue
        for horizon in _HORIZONS:
            horizon_key = f"{horizon}d"
            horizon_decision = decision.get("horizons", {}).get(horizon_key) or {}
            if horizon_decision.get("status") == "prediction":
                continue
            matching = [
                policy
                for policy in policies_by_horizon.get(horizon_key, [])
                if _coverage_repair_policy_matches(row, horizon_decision, horizon, policy)
            ]
            if not matching:
                continue
            policy = sorted(
                matching,
                key=lambda item: (item["precision"], item["match_count"], len(item["fields"])),
                reverse=True,
            )[0]
            decision["horizons"][horizon_key] = _coverage_repair_horizon(feature_df, idx, horizon, policy)


def _coverage_repair_policies_by_horizon(backtest_1y: dict) -> dict[str, list[dict]]:
    policies_by_horizon: dict[str, list[dict]] = {}
    for horizon_key, result in backtest_1y.items():
        if isinstance(result, dict):
            policies_by_horizon[horizon_key] = deepcopy(result.get("coverage_repair_policies") or [])
    return policies_by_horizon


def _direction_gate_blocked_horizon(horizon_key: str, original: dict, gate: dict) -> dict:
    horizon = _horizon_from_key(horizon_key)
    model_meta = original.get("model", {}) if isinstance(original, dict) else {}
    blocked = _no_prediction_horizon(
        horizon,
        "direction_quality_gate_failed",
        training_sample_count=int(_safe_num(model_meta.get("training_sample_count"), 0)),
        positive_count=int(_safe_num(model_meta.get("continuation_training_count"), 0)),
        negative_count=int(_safe_num(model_meta.get("reversal_training_count"), 0)),
    )
    blocked["deployment_quality_gate"] = deepcopy(gate)
    blocked["blocked_prediction"] = {
        "status": original.get("status"),
        "predicted_direction": original.get("predicted_direction"),
        "continuation_probability": original.get("continuation_probability"),
        "reversal_probability": original.get("reversal_probability"),
        "confidence_score": original.get("confidence_score"),
        "no_prediction_reason": original.get("no_prediction_reason"),
    }
    blocked["model"] = deepcopy(model_meta) if model_meta else blocked["model"]
    return blocked


def _apply_direction_quality_gates_to_decision(decision: dict, gates: dict[str, dict]) -> dict:
    if not decision:
        return decision
    horizons = decision.get("horizons", {})
    for horizon_key, horizon_decision in list(horizons.items()):
        if not isinstance(horizon_decision, dict):
            continue
        direction = horizon_decision.get("predicted_direction")
        horizon_gate = gates.get(horizon_key, {})
        signal_key = _signal_gate_key(direction, _prediction_signal_id_from_horizon(horizon_decision))
        signal_gate = (horizon_gate.get("signals") or {}).get(signal_key)
        direction_gate = horizon_gate.get(direction) if direction in ("continuation", "reversal") else None
        active_gate = signal_gate or direction_gate
        if horizon_decision.get("status") != "prediction" or not isinstance(direction_gate, dict):
            horizon_decision["deployment_quality_gate"] = deepcopy(horizon_gate) if horizon_gate else None
            continue
        if not active_gate.get("deployment_enabled", False):
            horizons[horizon_key] = _direction_gate_blocked_horizon(horizon_key, horizon_decision, active_gate)
        else:
            horizon_decision["deployment_quality_gate"] = deepcopy(active_gate)
    return decision


def _apply_deployment_quality_gates_to_decision(decision: dict, gates: dict[str, dict]) -> dict:
    if not decision:
        return decision
    horizons = decision.get("horizons", {})
    for horizon_key, gate in gates.items():
        if gate.get("deployment_enabled", True):
            if horizon_key in horizons and isinstance(horizons[horizon_key], dict):
                horizons[horizon_key]["deployment_quality_gate"] = deepcopy(gate)
            continue
        original = horizons.get(horizon_key)
        if not isinstance(original, dict):
            continue
        horizons[horizon_key] = _quality_gate_blocked_horizon(horizon_key, original, gate)
    return decision


def _finalize_deployment_quality(
    feature_df: pd.DataFrame,
    decisions_by_index: dict[int, dict],
) -> tuple[dict[int, dict], dict]:
    raw_backtest = run_decision_backtest(feature_df, decisions_by_index=decisions_by_index)
    direction_gates = _direction_quality_gates(raw_backtest)

    gated_decisions = deepcopy(decisions_by_index)
    for decision in gated_decisions.values():
        _apply_direction_quality_gates_to_decision(decision, direction_gates)

    baseline_backtest = run_decision_backtest(feature_df, decisions_by_index=gated_decisions)
    repair_policies_by_horizon: dict[str, list[dict]] = {}
    repair_counts_by_horizon: dict[str, int] = {}
    for horizon in _HORIZONS:
        horizon_key = f"{horizon}d"
        baseline_result = baseline_backtest.get(horizon_key, {})
        selected_rows = list(baseline_result.get("predictions", []))
        repair_candidates = _coverage_repair_candidates(feature_df, gated_decisions, horizon)
        selected_repair_candidates = _select_max_safe_coverage_candidates(selected_rows, repair_candidates)
        repair_policies_by_horizon[horizon_key] = [
            candidate["policy"]
            for candidate in selected_repair_candidates
        ]
        repair_counts_by_horizon[horizon_key] = len(
            {
                _prediction_row_key(row)
                for candidate in selected_repair_candidates
                for row in candidate["rows"]
            }
        )
    _apply_coverage_repair_policies(feature_df, gated_decisions, repair_policies_by_horizon)

    final_backtest = run_decision_backtest(feature_df, decisions_by_index=gated_decisions)
    for horizon_key, result in final_backtest.items():
        raw_result = raw_backtest.get(horizon_key, {})
        result["quality_gate"] = _deployment_quality_gate(result)
        result["direction_quality_gate"] = deepcopy(direction_gates.get(horizon_key, {}))
        result["raw_prediction_count"] = raw_result.get("prediction_count")
        result["raw_accuracy"] = raw_result.get("accuracy")
        result["raw_reverse_accuracy"] = raw_result.get("reversal_accuracy")
        result["raw_continue_accuracy"] = raw_result.get("continuation_accuracy")
        result["coverage_policy"] = raw_result.get("coverage_policy", _COVERAGE_POLICY_MAX_SAFE)
        result["max_safe_prediction_count"] = result.get("prediction_count")
        result["max_safe_coverage"] = result.get("coverage")
        result["coverage_expansion_signal_count"] = raw_result.get("coverage_expansion_signal_count", 0)
        result["coverage_repair_policy_count"] = len(repair_policies_by_horizon.get(horizon_key, []))
        result["coverage_repair_prediction_count"] = repair_counts_by_horizon.get(horizon_key, 0)
        result["coverage_repair_policies"] = deepcopy(repair_policies_by_horizon.get(horizon_key, []))
    return gated_decisions, final_backtest


def _adaptive_profile_key(profile: dict) -> tuple:
    return (
        profile["id"],
        int(profile["top_features"]),
        int(profile["neighbors"]),
        bool(profile["same_side"]),
        float(profile["reversal_confidence"]),
        float(profile["continuation_confidence"]),
        tuple(profile.get("allowed_directions") or ()),
    )


def _adaptive_profiles_for_horizon(horizon: int) -> list[dict]:
    if horizon <= 5:
        core_ids = ("compact_6x20", "strict_6x10", "same_side_8x10")
        expansion_ids = ("strict_same_side_4x10", "same_side_8x8", "balanced_16x16", "same_side_12x12")
        opportunity_ids = ("trend_8x24", "trend_12x24", "same_side_trend_6x16")
    else:
        core_ids = ("same_side_8x8", "strict_same_side_4x10", "balanced_16x16")
        expansion_ids = ("broad_24x16", "strict_24x20", "same_side_12x12", "broad_24x12")
        opportunity_ids = ("trend_8x24", "trend_12x24", "same_side_trend_6x16", "broad_consensus_32x24")

    profiles: list[dict] = []
    for profile in _ADAPTIVE_ANALOG_PROFILES:
        if profile["id"] in core_ids:
            item = deepcopy(profile)
            item["tier"] = "core"
            profiles.append(item)
        elif profile["id"] in expansion_ids:
            item = deepcopy(profile)
            item["tier"] = "expansion"
            profiles.append(item)
        elif profile["id"] in opportunity_ids:
            item = deepcopy(profile)
            item["tier"] = "opportunity"
            profiles.append(item)
    return profiles


def _adaptive_feature_columns(feature_df: pd.DataFrame) -> list[str]:
    return [
        feature
        for feature in _MODEL_FEATURES
        if feature in feature_df.columns and feature != "event_risk_blocked"
    ]


def _adaptive_state(feature_df: pd.DataFrame, horizon: int) -> dict:
    cache = feature_df.attrs.get("_adaptive_analog_state")
    if not isinstance(cache, _NoDeepcopyDict):
        cache = _NoDeepcopyDict(cache or {})
        feature_df.attrs["_adaptive_analog_state"] = cache
    if horizon in cache:
        return cache[horizon]

    feature_columns = _adaptive_feature_columns(feature_df)
    if feature_columns:
        x_values = feature_df[feature_columns].apply(pd.to_numeric, errors="coerce").to_numpy(dtype=float)
        x_values = np.nan_to_num(x_values, nan=0.0, posinf=0.0, neginf=0.0)
    else:
        x_values = np.empty((len(feature_df), 0), dtype=float)

    labels = np.full(len(feature_df), np.nan, dtype=float)
    for idx in range(len(feature_df)):
        label = _label_for_index(feature_df, idx, horizon)
        if label is not None:
            labels[idx] = float(label)

    side_sign = (
        pd.to_numeric(feature_df.get("analysis_side_sign", pd.Series(0.0, index=feature_df.index)), errors="coerce")
        .fillna(0.0)
        .to_numpy(dtype=float)
    )
    touch_weight = np.asarray(
        [1.0 if side in ("Upper", "Lower") else 0.0 for side in feature_df.get("touched_side", [])],
        dtype=float,
    )

    state = {
        "feature_columns": feature_columns,
        "x": x_values,
        "labels": labels,
        "valid_labels": np.isfinite(labels),
        "side_sign": side_sign,
        "touch_weight": touch_weight,
        "row_context_cache": {},
        "raw_cache": {},
    }
    cache[horizon] = state
    return state


def _adaptive_training_indices(state: dict, target_idx: int, horizon: int) -> np.ndarray:
    known_limit = target_idx - horizon
    if known_limit < 0:
        return np.asarray([], dtype=int)
    all_indices = np.arange(len(state["labels"]), dtype=int)
    return all_indices[state["valid_labels"] & (all_indices <= known_limit)]


def _adaptive_row_context(feature_df: pd.DataFrame, row_index: int, horizon: int) -> dict | None:
    state = _adaptive_state(feature_df, horizon)
    if row_index in state["row_context_cache"]:
        return state["row_context_cache"][row_index]

    feature_columns = state["feature_columns"]
    if not feature_columns:
        state["row_context_cache"][row_index] = None
        return None

    train_idx = _adaptive_training_indices(state, row_index, horizon)
    if len(train_idx) < _ADAPTIVE_MIN_TRAINING_ROWS:
        state["row_context_cache"][row_index] = None
        return None

    y_train = state["labels"][train_idx].astype(int)
    if len(set(y_train.tolist())) < 2:
        state["row_context_cache"][row_index] = None
        return None

    x_train = state["x"][train_idx]
    median = np.median(x_train, axis=0)
    mad = np.median(np.abs(x_train - median), axis=0)
    mad = np.where(mad < 1e-6, 1.0, mad)
    z_train = np.clip((x_train - median) / mad, -6.0, 6.0)
    z_target = np.clip((state["x"][row_index] - median) / mad, -6.0, 6.0)

    positive_median = np.median(z_train[y_train == 1], axis=0)
    negative_median = np.median(z_train[y_train == 0], axis=0)
    feature_strength = np.nan_to_num(np.abs(positive_median - negative_median), nan=0.0)
    context = {
        "train_idx": train_idx,
        "y_train": y_train,
        "z_train": z_train,
        "z_target": z_target,
        "feature_strength": feature_strength,
        "training_count": int(len(train_idx)),
        "positive_count": int(np.sum(y_train == 1)),
        "negative_count": int(np.sum(y_train == 0)),
    }
    state["row_context_cache"][row_index] = context
    return context


def _adaptive_raw_profile_prediction(
    feature_df: pd.DataFrame,
    row_index: int,
    horizon: int,
    profile: dict,
) -> dict | None:
    state = _adaptive_state(feature_df, horizon)
    cache_key = (int(row_index), _adaptive_profile_key(profile))
    if cache_key in state["raw_cache"]:
        return state["raw_cache"][cache_key]

    feature_columns = state["feature_columns"]
    context = _adaptive_row_context(feature_df, row_index, horizon)
    if context is None:
        state["raw_cache"][cache_key] = None
        return None

    train_idx = context["train_idx"]
    y_train = context["y_train"]
    z_train = context["z_train"]
    z_target = context["z_target"]
    feature_strength = context["feature_strength"]
    top_count = min(max(1, int(profile["top_features"])), len(feature_columns))
    selected_indices = np.argsort(feature_strength)[-top_count:]

    analog_mask = np.ones(len(train_idx), dtype=bool)
    if profile.get("same_side"):
        target_side = np.sign(state["side_sign"][row_index])
        same_side_mask = np.sign(state["side_sign"][train_idx]) == target_side
        if int(same_side_mask.sum()) >= 40:
            analog_mask = same_side_mask

    analog_z = z_train[analog_mask]
    analog_y = y_train[analog_mask]
    analog_idx = train_idx[analog_mask]
    neighbor_count = min(int(profile["neighbors"]), len(analog_idx))
    if neighbor_count <= 0:
        state["raw_cache"][cache_key] = None
        return None

    distances = np.mean((analog_z[:, selected_indices] - z_target[selected_indices]) ** 2, axis=1)
    order = np.argsort(distances)[:neighbor_count]
    neighbor_idx = analog_idx[order]
    neighbor_labels = analog_y[order]
    neighbor_distances = distances[order]
    similarity = np.exp(-neighbor_distances / 2.0)
    age_scale = max(1, int(analog_idx.max()) - int(analog_idx.min()))
    recency = (neighbor_idx - int(analog_idx.min())) / age_scale
    weights = similarity * (1.0 + (0.80 * recency)) * (1.0 + (0.35 * state["touch_weight"][neighbor_idx]))
    continuation_probability = _clamp(float(np.dot(weights, neighbor_labels) / (weights.sum() + _EPS)), 0.01, 0.99)
    reversal_probability = 1.0 - continuation_probability
    if continuation_probability >= reversal_probability:
        direction = "continuation"
        confidence = continuation_probability
        required_confidence = float(profile["continuation_confidence"])
    else:
        direction = "reversal"
        confidence = reversal_probability
        required_confidence = float(profile["reversal_confidence"])

    expected_label = 1 if direction == "continuation" else 0
    analog_agreement = float(np.mean(neighbor_labels == expected_label))
    selected_features = sorted(
        (
            {
                "feature": feature_columns[int(feature_idx)],
                "strength": round(float(feature_strength[int(feature_idx)]), 6),
                "target_value": _value_for_payload(state["x"][row_index, int(feature_idx)]),
            }
            for feature_idx in selected_indices
        ),
        key=lambda item: item["strength"],
        reverse=True,
    )

    neighbors = []
    for idx, label, distance in zip(neighbor_idx[:8], neighbor_labels[:8], neighbor_distances[:8], strict=False):
        neighbors.append(
            {
                "date": _to_date_string(feature_df.iloc[int(idx)].get("date")),
                "direction": "continuation" if int(label) == 1 else "reversal",
                "distance": round(float(distance), 6),
                "touched_side": feature_df.iloc[int(idx)].get("touched_side"),
            }
        )

    result = {
        "profile": profile,
        "direction": direction,
        "continuation_probability": continuation_probability,
        "reversal_probability": reversal_probability,
        "confidence": confidence,
        "required_confidence": required_confidence,
        "analog_agreement": analog_agreement,
        "status": confidence >= required_confidence and analog_agreement >= required_confidence,
        "neighbor_count": int(neighbor_count),
        "training_count": context["training_count"],
        "positive_count": context["positive_count"],
        "negative_count": context["negative_count"],
        "selected_features": selected_features[:10],
        "neighbors": neighbors,
    }
    state["raw_cache"][cache_key] = result
    return result


def _adaptive_reversal_veto_reason(row: pd.Series, horizon: int) -> str | None:
    side = row.get("touched_side")
    side_ret_5d = _safe_num(row.get("side_ret_5d"), 0.0)
    side_ret_10d = _safe_num(row.get("side_ret_10d"), 0.0)
    side_qqq = _safe_num(row.get("side_qqq_ret_5d"), 0.0)
    side_xlk = _safe_num(row.get("side_xlk_ret_5d"), 0.0)
    pressure = _safe_num(row.get("side_weighted_volume_pressure_5"), 0.0)
    adx = _safe_num(row.get("ADX14"), 0.0)
    band_rank = _safe_num(row.get("band_width_percentile"), 0.5)
    bandwidth_change = _safe_num(row.get("bandwidth_change_5d"), 0.0)
    consecutive = _safe_num(row.get("consecutive_touch_count"), 0.0)
    reentry = _safe_num(row.get("touch_reentry_signal"), 0.0)
    wick_minus_body = _safe_num(row.get("touch_wick_minus_body"), 0.0)
    close_location = _safe_num(row.get("side_close_location"), 0.0)
    touch_depth = _safe_num(row.get("touch_depth_atr"), 0.0)

    if side == "Upper":
        broad_breakout = side_qqq > 0.035 and side_xlk > 0.035 and side_ret_5d > 0.045 and pressure > 0.10
        if broad_breakout and adx < 24.0:
            return "broad_breakout_without_exhaustion"
        if adx < 20.0 and pressure > 0.15 and touch_depth < 0.50 and side_ret_10d < 0.07:
            return "early_breakout_no_exhaustion"
        if (
            band_rank > 0.75
            and bandwidth_change > 0.08
            and side_ret_10d > 0.12
            and consecutive >= 3
            and pressure > 0.30
        ):
            return "expanding_band_breakout"
        clean_close_outside = reentry < 0.0 and wick_minus_body < -0.25 and close_location > 0.50
        if clean_close_outside and adx < 24.0 and not (band_rank > 0.85 and side_ret_10d > 0.15):
            return "no_rejection_breakout_close"

    if side == "Lower":
        falling_thrust = side_ret_5d > 0.04 or side_ret_10d > 0.06
        weak_exhaustion = pressure < 0.20 and wick_minus_body < 0.25 and touch_depth < 0.70
        early_or_outside = consecutive < 6 or reentry < 0.0
        market_not_helping = side_qqq > 0.005 or side_xlk > 0.005
        if falling_thrust and weak_exhaustion and early_or_outside and market_not_helping:
            return "falling_knife_no_exhaustion"

    return None


def _adaptive_continuation_veto_reason(row: pd.Series, horizon: int) -> str | None:
    side = row.get("touched_side")
    side_ret_5d = _safe_num(row.get("side_ret_5d"), 0.0)
    side_ret_10d = _safe_num(row.get("side_ret_10d"), 0.0)
    side_ret_20d = _safe_num(row.get("side_ret_20d"), 0.0)
    side_rsi = _safe_num(row.get("side_rsi_deviation"), 0.0)
    side_qqq = _safe_num(row.get("side_qqq_ret_5d"), 0.0)
    pressure = _safe_num(row.get("side_weighted_volume_pressure_5"), 0.0)
    rel_volume = _safe_num(row.get("rel_volume_20"), 1.0)
    band_rank = _safe_num(row.get("band_width_percentile"), 0.5)
    bandwidth_change = _safe_num(row.get("bandwidth_change_5d"), 0.0)
    consecutive = _safe_num(row.get("consecutive_touch_count"), 0.0)
    reentry = _safe_num(row.get("touch_reentry_signal"), 0.0)
    wick_minus_body = _safe_num(row.get("touch_wick_minus_body"), 0.0)
    close_location = _safe_num(row.get("side_close_location"), 0.0)

    rejection_close = reentry > 0.0 and wick_minus_body >= 0.28 and close_location <= 0.25
    if rejection_close:
        return "continuation_rejected_at_band"

    if side == "Upper":
        overextended = side_ret_10d > 0.12 or side_ret_20d > 0.18
        exhaustion = side_rsi > 0.70 and (wick_minus_body >= 0.18 or pressure < -0.05)
        fragile_squeeze_release = band_rank > 0.85 and bandwidth_change < 0.02 and pressure < 0.10
        if horizon >= 10 and overextended and exhaustion and not fragile_squeeze_release:
            return "upper_exhaustion_reversal_risk"

    if side == "Lower":
        capitulation = (
            consecutive >= 4
            and side_ret_5d > 0.045
            and (pressure >= 0.25 or rel_volume >= 1.30)
            and wick_minus_body >= 0.12
            and side_qqq <= 0.015
        )
        if capitulation:
            return "lower_capitulation_reversal_risk"

    return None


def _adaptive_profile_thresholds(profile: dict, direction: str) -> tuple[float, float, float]:
    min_precision = (
        _ADAPTIVE_REVERSAL_PRECISION_THRESHOLD
        if direction == "reversal"
        else _ADAPTIVE_CONTINUATION_PRECISION_THRESHOLD
    )
    min_wilson = _ADAPTIVE_MIN_WILSON
    min_confidence = float(
        profile["reversal_confidence"] if direction == "reversal" else profile["continuation_confidence"]
    )
    tier = profile.get("tier", "core")

    if tier == "expansion":
        if direction == "reversal":
            min_precision = max(min_precision, _ADAPTIVE_EXPANSION_REVERSAL_MIN_PRECISION)
            min_wilson = max(min_wilson, _ADAPTIVE_EXPANSION_REVERSAL_MIN_WILSON)
            min_confidence = max(min_confidence, _ADAPTIVE_EXPANSION_REVERSAL_MIN_CONFIDENCE)
        else:
            min_precision = max(min_precision, _ADAPTIVE_EXPANSION_CONTINUATION_MIN_PRECISION)
            min_wilson = min(min_wilson, _ADAPTIVE_EXPANSION_CONTINUATION_MIN_WILSON)
            min_confidence = max(min_confidence, _ADAPTIVE_EXPANSION_CONTINUATION_MIN_CONFIDENCE)
    elif tier == "opportunity":
        if direction != "continuation":
            return 1.01, 1.01, 1.01
        min_precision = max(min_precision, _ADAPTIVE_OPPORTUNITY_CONTINUATION_MIN_PRECISION)
        min_wilson = min(min_wilson, _ADAPTIVE_OPPORTUNITY_CONTINUATION_MIN_WILSON)
        min_confidence = max(min_confidence, _ADAPTIVE_OPPORTUNITY_CONTINUATION_MIN_CONFIDENCE)

    return min_precision, min_wilson, min_confidence


def _empirical_bucket(value: float, cuts: tuple[float, float, float, float]) -> str:
    if value <= cuts[0]:
        return "very_low"
    if value <= cuts[1]:
        return "low"
    if value <= cuts[2]:
        return "neutral"
    if value <= cuts[3]:
        return "high"
    return "very_high"


def _empirical_regime_attrs(row: pd.Series, horizon: int) -> dict[str, str]:
    side = row.get("touched_side")
    reentry = _safe_num(row.get("touch_reentry_signal"), 0.0)
    wick_minus_body = _safe_num(row.get("touch_wick_minus_body"), 0.0)
    close_location = _safe_num(row.get("side_close_location"), 0.0)
    consecutive = _safe_num(row.get("consecutive_touch_count"), 0.0)
    side_ret_5d = _safe_num(row.get("side_ret_5d"), 0.0)
    side_ret_10d = _safe_num(row.get("side_ret_10d"), 0.0)
    side_ret_20d = _safe_num(row.get("side_ret_20d"), 0.0)
    side_qqq = _safe_num(row.get("side_qqq_ret_5d"), 0.0)
    side_xlk = _safe_num(row.get("side_xlk_ret_5d"), 0.0)
    pressure = _safe_num(row.get("side_weighted_volume_pressure_5"), 0.0)
    rel_volume = _safe_num(row.get("rel_volume_20"), 1.0)
    band_rank = _safe_num(row.get("band_width_percentile"), 0.5)
    bandwidth_change = _safe_num(row.get("bandwidth_change_5d"), 0.0)
    touch_depth = _safe_num(row.get("touch_depth_atr"), 0.0)
    adx = _safe_num(row.get("ADX14"), 0.0)

    if reentry > 0.0 and (wick_minus_body >= 0.10 or close_location <= 0.30):
        touch_quality = "rejection"
    elif reentry <= 0.0 and close_location >= 0.35 and wick_minus_body < 0.18:
        touch_quality = "outside_close"
    elif touch_depth >= 0.60:
        touch_quality = "deep_touch"
    else:
        touch_quality = "mixed_touch"

    if consecutive >= 5:
        cluster = "extended_walk"
    elif consecutive >= 3:
        cluster = "walk"
    elif consecutive >= 2:
        cluster = "second_touch"
    else:
        cluster = "single_touch"

    trend_value = side_ret_10d if horizon >= 10 else side_ret_5d
    if horizon >= 10:
        trend = _empirical_bucket(trend_value, (-0.025, 0.015, 0.055, 0.105))
    else:
        trend = _empirical_bucket(trend_value, (-0.015, 0.010, 0.035, 0.075))

    longer_trend = _empirical_bucket(side_ret_20d, (-0.040, 0.010, 0.070, 0.145))
    market_value = (side_qqq + side_xlk) / 2.0
    market = _empirical_bucket(market_value, (-0.020, -0.004, 0.008, 0.025))
    volume_pressure = _empirical_bucket(pressure, (-0.25, -0.05, 0.12, 0.32))
    participation = "high_volume" if rel_volume >= 1.25 else "dry_volume" if rel_volume <= 0.80 else "normal_volume"
    band_state = (
        "wide_expanding"
        if band_rank >= 0.75 and bandwidth_change >= 0.02
        else "wide_stalling"
        if band_rank >= 0.75
        else "compressed"
        if band_rank <= 0.30
        else "normal_band"
    )
    adx_state = "strong_trend" if adx >= 28 else "weak_trend" if adx <= 18 else "moderate_trend"

    return {
        "side": str(side),
        "touch_quality": touch_quality,
        "cluster": cluster,
        "trend": trend,
        "longer_trend": longer_trend,
        "market": market,
        "volume_pressure": volume_pressure,
        "participation": participation,
        "band_state": band_state,
        "adx_state": adx_state,
    }


def _empirical_regime_specs(horizon: int) -> list[tuple[str, tuple[str, ...], int]]:
    base = [
        ("side_touch_cluster_trend", ("side", "touch_quality", "cluster", "trend"), 4),
        ("side_touch_cluster", ("side", "touch_quality", "cluster"), 5),
        ("side_touch_trend", ("side", "touch_quality", "trend"), 5),
        ("side_cluster_trend", ("side", "cluster", "trend"), 5),
        ("side_touch_market", ("side", "touch_quality", "market"), 5),
        ("side_trend_market", ("side", "trend", "market"), 6),
        ("side_touch", ("side", "touch_quality"), 6),
        ("side_cluster", ("side", "cluster"), 7),
        ("side_trend", ("side", "trend"), 7),
        ("side_market", ("side", "market"), 8),
        ("side_only", ("side",), 8),
    ]
    if horizon >= 10:
        return base + [
            ("side_longer_trend", ("side", "longer_trend"), 8),
            ("side_band_state", ("side", "band_state"), 8),
        ]
    return base


def _empirical_rows_for_scope(
    feature_df: pd.DataFrame,
    target_attrs: dict[str, str],
    *,
    row_index: int,
    horizon: int,
    fields: tuple[str, ...],
) -> list[dict]:
    max_idx = min(row_index - horizon, len(feature_df) - horizon - 1)
    if max_idx < 0:
        return []

    matches: list[dict] = []
    for idx in range(max_idx + 1):
        row = feature_df.iloc[idx]
        if _safe_bool(row.get("event_risk_blocked")):
            continue
        if row.get("touched_side") not in ("Upper", "Lower"):
            continue
        attrs = _empirical_regime_attrs(row, horizon)
        if any(attrs.get(field) != target_attrs.get(field) for field in fields):
            continue
        actual = _actual_direction_for_index(feature_df, idx, horizon)
        if actual not in ("continuation", "reversal"):
            continue
        matches.append(
            {
                "idx": idx,
                "date": _to_date_string(row.get("date")),
                "direction": actual,
                "touched_side": row.get("touched_side"),
            }
        )
    return matches[-_EMPIRICAL_REGIME_MAX_MATCHES:]


def _score_empirical_regime_direction(
    rows: list[dict],
    *,
    direction: str,
    min_matches: int,
    scope: str,
    fields: tuple[str, ...],
) -> dict | None:
    count = len(rows)
    if count < min_matches:
        return None
    correct = sum(1 for row in rows if row.get("direction") == direction)
    precision = correct / count
    posterior = (correct + 1.0) / (count + 2.0)
    wilson = _wilson_lower_bound(correct, count)
    recent_rows = rows[-min(_EMPIRICAL_REGIME_RECENT_MATCHES, count) :]
    recent_correct = sum(1 for row in recent_rows if row.get("direction") == direction)
    recent_precision = recent_correct / len(recent_rows) if recent_rows else precision

    if direction == "reversal":
        min_precision = _EMPIRICAL_REVERSAL_MIN_PRECISION
        min_wilson = _EMPIRICAL_REVERSAL_MIN_WILSON
        min_recent = 0.78
    else:
        min_precision = _EMPIRICAL_CONTINUATION_MIN_PRECISION
        min_wilson = _EMPIRICAL_CONTINUATION_MIN_WILSON
        min_recent = 0.72

    if precision < min_precision or wilson < min_wilson or recent_precision < min_recent:
        return None

    specificity = min(1.0, len(fields) / 4.0)
    sample_strength = min(1.0, math.log1p(count) / math.log1p(_EMPIRICAL_REGIME_MAX_MATCHES))
    score = (
        (0.42 * precision)
        + (0.24 * wilson)
        + (0.18 * recent_precision)
        + (0.08 * posterior)
        + (0.05 * sample_strength)
        + (0.03 * specificity)
    )
    return {
        "status": "ready",
        "source": "empirical_regime",
        "direction": direction,
        "scope": scope,
        "fields": list(fields),
        "match_count": count,
        "correct_count": correct,
        "precision": precision,
        "posterior_probability": posterior,
        "wilson_lower_bound": wilson,
        "recent_precision": recent_precision,
        "score": score,
        "neighbors": rows[-8:],
    }


def _score_recent_empirical_regime_direction(
    rows: list[dict],
    *,
    direction: str,
    scope: str,
    fields: tuple[str, ...],
) -> list[dict]:
    if direction not in ("continuation", "reversal"):
        return []

    out: list[dict] = []
    min_precision = (
        _EMPIRICAL_RECENT_REVERSAL_MIN_PRECISION
        if direction == "reversal"
        else _EMPIRICAL_RECENT_CONTINUATION_MIN_PRECISION
    )
    min_wilson = (
        _EMPIRICAL_RECENT_REVERSAL_MIN_WILSON
        if direction == "reversal"
        else _EMPIRICAL_RECENT_CONTINUATION_MIN_WILSON
    )

    for window in _EMPIRICAL_RECENT_WINDOWS:
        if len(rows) < window:
            continue
        recent_rows = rows[-window:]
        correct = sum(1 for row in recent_rows if row.get("direction") == direction)
        precision = correct / window
        wilson = _wilson_lower_bound(correct, window)
        if precision < min_precision or wilson < min_wilson:
            continue
        posterior = (correct + 1.0) / (window + 2.0)
        full_correct = sum(1 for row in rows if row.get("direction") == direction)
        full_precision = full_correct / len(rows) if rows else precision
        specificity = min(1.0, len(fields) / 4.0)
        score = (
            (0.45 * precision)
            + (0.22 * wilson)
            + (0.14 * posterior)
            + (0.10 * min(1.0, window / 12.0))
            + (0.06 * specificity)
            + (0.03 * full_precision)
        )
        out.append(
            {
                "status": "ready",
                "source": "recent_empirical_regime",
                "direction": direction,
                "scope": f"recent_{scope}_{window}",
                "base_scope": scope,
                "fields": list(fields),
                "match_count": window,
                "full_match_count": len(rows),
                "correct_count": correct,
                "precision": precision,
                "posterior_probability": posterior,
                "wilson_lower_bound": wilson,
                "recent_precision": precision,
                "full_precision": full_precision,
                "score": score,
                "neighbors": recent_rows[-8:],
            }
        )
    return out


def _score_empirical_regime(feature_df: pd.DataFrame, row_index: int, horizon: int) -> dict | None:
    row = feature_df.iloc[row_index]
    if row.get("touched_side") not in ("Upper", "Lower"):
        return None
    blocked_directions = {
        direction
        for direction, reason in (
            ("continuation", _adaptive_continuation_veto_reason(row, horizon)),
            ("reversal", _adaptive_reversal_veto_reason(row, horizon)),
        )
        if reason is not None
    }
    target_attrs = _empirical_regime_attrs(row, horizon)
    candidates: list[dict] = []
    for scope, fields, min_matches in _empirical_regime_specs(horizon):
        rows = _empirical_rows_for_scope(
            feature_df,
            target_attrs,
            row_index=row_index,
            horizon=horizon,
            fields=fields,
        )
        reverse_min_matches = max(min_matches, _EMPIRICAL_REVERSAL_MIN_MATCHES)
        continue_min_matches = max(min_matches, _EMPIRICAL_CONTINUATION_MIN_MATCHES)
        for direction, required_matches in (
            ("reversal", reverse_min_matches),
            ("continuation", continue_min_matches),
        ):
            if direction in blocked_directions:
                continue
            scored = _score_empirical_regime_direction(
                rows,
                direction=direction,
                min_matches=required_matches,
                scope=scope,
                fields=fields,
            )
            if scored is not None:
                candidates.append(scored)
            candidates.extend(
                _score_recent_empirical_regime_direction(
                    rows,
                    direction=direction,
                    scope=scope,
                    fields=fields,
                )
            )

    if not candidates:
        return None

    return sorted(
        candidates,
        key=lambda item: (
            item["score"],
            item["precision"],
            item["match_count"],
            1 if item["direction"] == "reversal" else 0,
        ),
        reverse=True,
    )[0]


def _empirical_regime_horizon(
    feature_df: pd.DataFrame,
    row_index: int,
    horizon: int,
    signal: dict,
    *,
    training_count: int,
    continuation_count: int,
    reversal_count: int,
    candidate_count: int,
) -> dict:
    direction = signal["direction"]
    posterior = _clamp(_safe_num(signal.get("posterior_probability"), 0.5), 0.01, 0.99)
    if direction == "continuation":
        continuation_probability = posterior
        reversal_probability = 1.0 - posterior
    else:
        reversal_probability = posterior
        continuation_probability = 1.0 - posterior

    confidence_score = int(round(max(continuation_probability, reversal_probability) * 100))
    model = _model_metadata(
        training_count,
        continuation_count,
        reversal_count,
        validation_accuracy=signal["precision"],
        validation_precision=signal["precision"],
        candidate_count=candidate_count,
    )
    model["type"] = "walk_forward_empirical_regime"
    playbook = {
        "id": f"empirical_{signal['scope']}",
        "name": "Empirical Regime Tape",
        "direction": direction,
        "tier": "regime",
        "match_count": signal["match_count"],
        "correct_count": signal["correct_count"],
        "precision": round(signal["precision"], 6),
        "posterior_probability": round(signal["posterior_probability"], 6),
        "wilson_lower_bound": round(signal["wilson_lower_bound"], 6),
        "recent_precision": round(signal["recent_precision"], 6),
        "score": round(signal["score"], 6),
        "scope": signal["scope"],
        "fields": signal["fields"],
        "neighbors": signal["neighbors"],
    }
    return {
        "status": "prediction",
        "predicted_direction": direction,
        "continuation_probability": round(continuation_probability, 6),
        "reversal_probability": round(reversal_probability, 6),
        "continuation_confidence_score": int(round(continuation_probability * 100)),
        "reversal_confidence_score": int(round(reversal_probability * 100)),
        "continuation_validation_precision": signal["precision"] if direction == "continuation" else None,
        "reversal_validation_precision": signal["precision"] if direction == "reversal" else None,
        "continuation_validation_count": signal["match_count"] if direction == "continuation" else 0,
        "reversal_validation_count": signal["match_count"] if direction == "reversal" else 0,
        "validation_policy": {
            "empirical_reversal_min_precision": _EMPIRICAL_REVERSAL_MIN_PRECISION,
            "empirical_continuation_min_precision": _EMPIRICAL_CONTINUATION_MIN_PRECISION,
            "empirical_reversal_min_wilson": _EMPIRICAL_REVERSAL_MIN_WILSON,
            "empirical_continuation_min_wilson": _EMPIRICAL_CONTINUATION_MIN_WILSON,
            "empirical_recent_reversal_min_precision": _EMPIRICAL_RECENT_REVERSAL_MIN_PRECISION,
            "empirical_recent_continuation_min_precision": _EMPIRICAL_RECENT_CONTINUATION_MIN_PRECISION,
            "scope": signal["scope"],
            "fields": signal["fields"],
        },
        "confidence_score": confidence_score,
        "threshold": _PREDICTION_THRESHOLD,
        "no_prediction_reason": None,
        "reversal_veto_reason": None,
        "analog_evidence": {
            "status": "ready",
            "direction": direction,
            "posterior_probability": playbook["posterior_probability"],
            "neighbor_count": signal["match_count"],
            "precision": playbook["precision"],
            "lower_bound": playbook["wilson_lower_bound"],
            "recent_precision": playbook["recent_precision"],
        },
        "analog_override": False,
        "deployment_quality_gate": None,
        "blocked_prediction": None,
        "playbook": playbook,
        "adaptive_candidates": [
            {
                "profile_id": playbook["id"],
                "status": "ready",
                "direction": direction,
                "tier": "regime",
                "precision": playbook["precision"],
                "validation_count": signal["match_count"],
                "confidence": round(max(continuation_probability, reversal_probability), 6),
                "blocked_reason": None,
            }
        ],
        "contributions": [
            {
                "horizon": f"{horizon}d",
                "feature": playbook["name"],
                "value": signal["match_count"],
                "impact": direction,
                "contribution": playbook["score"],
            }
        ],
        "model": model,
    }


def _adaptive_validation_indices(feature_df: pd.DataFrame, row_index: int, horizon: int) -> list[int]:
    start = max(0, row_index - _ADAPTIVE_VALIDATION_LOOKBACK_ROWS)
    indices: list[int] = []
    for idx in range(start, row_index):
        row = feature_df.iloc[idx]
        if row.get("touched_side") not in ("Upper", "Lower"):
            continue
        if _safe_bool(row.get("event_risk_blocked")):
            continue
        if idx + horizon > row_index:
            continue
        if _actual_direction_for_index(feature_df, idx, horizon) not in ("continuation", "reversal"):
            continue
        indices.append(idx)
    return indices[-_ADAPTIVE_MAX_VALIDATION_TOUCHES:]


def _score_adaptive_profile(
    feature_df: pd.DataFrame,
    row_index: int,
    horizon: int,
    profile: dict,
) -> dict | None:
    raw = _adaptive_raw_profile_prediction(feature_df, row_index, horizon, profile)
    if raw is None or not raw["status"]:
        return None

    direction = raw["direction"]
    allowed_directions = profile.get("allowed_directions")
    if allowed_directions and direction not in allowed_directions:
        return {
            "status": "blocked",
            "blocked_reason": "profile_direction_filter",
            "raw": raw,
            "profile": profile,
        }

    if direction == "continuation":
        veto_reason = _adaptive_continuation_veto_reason(feature_df.iloc[row_index], horizon)
        if veto_reason is not None:
            return {
                "status": "blocked",
                "blocked_reason": veto_reason,
                "raw": raw,
                "profile": profile,
            }

    if direction == "reversal":
        veto_reason = _adaptive_reversal_veto_reason(feature_df.iloc[row_index], horizon)
        if veto_reason is not None:
            return {
                "status": "blocked",
                "blocked_reason": veto_reason,
                "raw": raw,
                "profile": profile,
            }

    validation_count = int(raw["neighbor_count"])
    correct_count = int(round(raw["analog_agreement"] * validation_count))
    precision = raw["analog_agreement"]
    wilson = _wilson_lower_bound(correct_count, validation_count) if validation_count else 0.0
    min_precision, min_wilson, min_confidence = _adaptive_profile_thresholds(profile, direction)
    passed = (
        validation_count > 0
        and precision >= min_precision
        and wilson >= min_wilson
        and raw["confidence"] >= min_confidence
    )
    recent_precision = precision
    score = (
        (0.50 * (precision or 0.0))
        + (0.25 * wilson)
        + (0.20 * raw["confidence"])
        + (0.05 * min(validation_count, 12) / 12.0)
    )
    return {
        "status": "ready" if passed else "unvalidated",
        "blocked_reason": None,
        "profile": profile,
        "raw": raw,
        "direction": direction,
        "validation_count": validation_count,
        "correct_count": correct_count,
        "precision": precision,
        "wilson_lower_bound": wilson,
        "recent_precision": recent_precision,
        "score": score,
        "tier": profile.get("tier", "core"),
    }


def _adaptive_training_summary(feature_df: pd.DataFrame, row_index: int, horizon: int) -> tuple[int, int, int]:
    state = _adaptive_state(feature_df, horizon)
    train_idx = _adaptive_training_indices(state, row_index, horizon)
    if len(train_idx) == 0:
        return 0, 0, 0
    labels = state["labels"][train_idx].astype(int)
    return len(labels), int(np.sum(labels == 1)), int(np.sum(labels == 0))


def _evaluate_horizon_with_adaptive_analogs(feature_df: pd.DataFrame, row_index: int, horizon: int) -> dict:
    training_count, continuation_count, reversal_count = _adaptive_training_summary(feature_df, row_index, horizon)
    profile_count = len(_adaptive_profiles_for_horizon(horizon))
    regime_count = len(_empirical_regime_specs(horizon))
    candidate_search_count = profile_count + regime_count
    if training_count < _ADAPTIVE_MIN_TRAINING_ROWS:
        result = _no_prediction_horizon(
            horizon,
            "insufficient_training_data",
            training_sample_count=training_count,
            positive_count=continuation_count,
            negative_count=reversal_count,
        )
        result["model"]["type"] = "walk_forward_adaptive_analog"
        result["model"]["candidate_search_count"] = candidate_search_count
        result["adaptive_candidates"] = []
        return result

    regime_signal = _score_empirical_regime(feature_df, row_index, horizon)
    if regime_signal is not None:
        return _empirical_regime_horizon(
            feature_df,
            row_index,
            horizon,
            regime_signal,
            training_count=training_count,
            continuation_count=continuation_count,
            reversal_count=reversal_count,
            candidate_count=candidate_search_count,
        )

    scored = [
        score
        for profile in _adaptive_profiles_for_horizon(horizon)
        for score in [_score_adaptive_profile(feature_df, row_index, horizon, profile)]
        if score is not None
    ]
    ready = [score for score in scored if score["status"] == "ready"]
    if not ready:
        blocked = [score for score in scored if score.get("status") == "blocked"]
        raw_hits = [score for score in scored if score.get("raw", {}).get("status")]
        reason = "unvalidated_adaptive_edge"
        if blocked and not any(score.get("status") == "unvalidated" for score in scored):
            reason = blocked[0].get("blocked_reason") or "reversal_veto"
        elif not raw_hits:
            reason = "low_adaptive_confidence"

        result = _no_prediction_horizon(
            horizon,
            reason,
            training_sample_count=training_count,
            positive_count=continuation_count,
            negative_count=reversal_count,
        )
        result["model"]["type"] = "walk_forward_adaptive_analog"
        result["model"]["candidate_search_count"] = candidate_search_count
        result["adaptive_candidates"] = [
            {
                "profile_id": item["profile"]["id"],
                "status": item["status"],
                "direction": item.get("direction") or item.get("raw", {}).get("direction"),
                "tier": item.get("tier") or item["profile"].get("tier"),
                "precision": _value_for_payload(item.get("precision")),
                "validation_count": item.get("validation_count", 0),
                "blocked_reason": item.get("blocked_reason"),
                "confidence": _value_for_payload(item.get("raw", {}).get("confidence")),
            }
            for item in sorted(scored, key=lambda candidate: candidate.get("score", 0.0), reverse=True)[:5]
        ]
        return result

    best = sorted(
        ready,
        key=lambda item: (item["score"], item["validation_count"], item["raw"]["confidence"]),
        reverse=True,
    )[0]
    raw = best["raw"]
    direction = best["direction"]
    continuation_probability = raw["continuation_probability"]
    reversal_probability = raw["reversal_probability"]
    confidence_score = int(round(max(continuation_probability, reversal_probability) * 100))
    precision = best["precision"] or 0.0
    model = _model_metadata(
        training_count,
        continuation_count,
        reversal_count,
        validation_accuracy=precision,
        validation_precision=precision,
        candidate_count=len(scored),
    )
    model["type"] = "walk_forward_adaptive_analog"
    model["candidate_search_count"] = candidate_search_count
    signal = {
        "id": f"adaptive_{best['profile']['id']}",
        "name": "Adaptive Analog Signal",
        "direction": direction,
        "tier": best.get("tier", best["profile"].get("tier", "core")),
        "match_count": best["validation_count"],
        "correct_count": best["correct_count"],
        "precision": round(precision, 6),
        "posterior_probability": round((best["correct_count"] + 1.0) / (best["validation_count"] + 2.0), 6),
        "wilson_lower_bound": round(best["wilson_lower_bound"], 6),
        "recent_precision": round(best["recent_precision"], 6) if best["recent_precision"] is not None else None,
        "score": round(best["score"], 6),
        "profile": deepcopy(best["profile"]),
        "selected_features": raw["selected_features"],
        "neighbors": raw["neighbors"],
    }
    selected_features = raw["selected_features"][:8]
    return {
        "status": "prediction",
        "predicted_direction": direction,
        "continuation_probability": round(continuation_probability, 6),
        "reversal_probability": round(reversal_probability, 6),
        "continuation_confidence_score": int(round(continuation_probability * 100)),
        "reversal_confidence_score": int(round(reversal_probability * 100)),
        "continuation_validation_precision": precision if direction == "continuation" else None,
        "reversal_validation_precision": precision if direction == "reversal" else None,
        "continuation_validation_count": best["validation_count"] if direction == "continuation" else 0,
        "reversal_validation_count": best["validation_count"] if direction == "reversal" else 0,
        "validation_policy": {
            "min_reversal_precision": _ADAPTIVE_REVERSAL_PRECISION_THRESHOLD,
            "min_continuation_precision": _ADAPTIVE_CONTINUATION_PRECISION_THRESHOLD,
            "min_wilson_lower_bound": _ADAPTIVE_MIN_WILSON,
            "expansion_reversal_min_precision": _ADAPTIVE_EXPANSION_REVERSAL_MIN_PRECISION,
            "expansion_continuation_min_precision": _ADAPTIVE_EXPANSION_CONTINUATION_MIN_PRECISION,
            "opportunity_continuation_min_precision": _ADAPTIVE_OPPORTUNITY_CONTINUATION_MIN_PRECISION,
            "validation_lookback_rows": _ADAPTIVE_VALIDATION_LOOKBACK_ROWS,
        },
        "confidence_score": confidence_score,
        "threshold": raw["required_confidence"],
        "no_prediction_reason": None,
        "reversal_veto_reason": None,
        "analog_evidence": {
            "status": "ready",
            "direction": direction,
            "posterior_probability": signal["posterior_probability"],
            "neighbor_count": raw["neighbor_count"],
            "precision": signal["precision"],
            "lower_bound": signal["wilson_lower_bound"],
            "analog_agreement": round(raw["analog_agreement"], 6),
        },
        "analog_override": False,
        "deployment_quality_gate": None,
        "blocked_prediction": None,
        "playbook": signal,
        "adaptive_candidates": [
            {
                "profile_id": item["profile"]["id"],
                "status": item["status"],
                "direction": item.get("direction") or item.get("raw", {}).get("direction"),
                "tier": item.get("tier") or item["profile"].get("tier"),
                "precision": _value_for_payload(item.get("precision")),
                "validation_count": item.get("validation_count", 0),
                "confidence": _value_for_payload(item.get("raw", {}).get("confidence")),
                "blocked_reason": item.get("blocked_reason"),
            }
            for item in sorted(scored, key=lambda candidate: candidate.get("score", 0.0), reverse=True)[:5]
        ],
        "contributions": [
            {
                "horizon": f"{horizon}d",
                "feature": item["feature"],
                "value": item["target_value"],
                "impact": direction,
                "contribution": item["strength"],
            }
            for item in selected_features
        ],
        "model": model,
    }


_PLAYBOOKS = [
    {
        "id": "lower_falling_knife_continue",
        "name": "Lower Band Falling Knife",
        "direction": "continuation",
    },
    {
        "id": "upper_breakout_continue",
        "name": "Upper Band Breakout",
        "direction": "continuation",
    },
    {
        "id": "band_walk_continue",
        "name": "Band Walk",
        "direction": "continuation",
    },
    {
        "id": "context_trend_continue",
        "name": "Market-Aligned Trend",
        "direction": "continuation",
    },
    {
        "id": "reentry_reversal",
        "name": "Band Re-Entry Reversal",
        "direction": "reversal",
    },
    {
        "id": "capitulation_reversal",
        "name": "Capitulation Reversal",
        "direction": "reversal",
    },
    {
        "id": "blowoff_reversal",
        "name": "Blow-Off Reversal",
        "direction": "reversal",
    },
    {
        "id": "stretched_reversal",
        "name": "Stretched Mean Reversion",
        "direction": "reversal",
    },
    {
        "id": "volume_rejection_reversal",
        "name": "Volume Rejection",
        "direction": "reversal",
    },
    {
        "id": "squeeze_release_continue",
        "name": "Squeeze Release",
        "direction": "continuation",
    },
]


def _playbook_by_id(playbook_id: str) -> dict | None:
    return next((playbook for playbook in _PLAYBOOKS if playbook["id"] == playbook_id), None)


def _playbook_condition(row: pd.Series, playbook_id: str, horizon: int) -> bool:
    side = row.get("touched_side")
    if side not in ("Upper", "Lower"):
        return False

    side_ret_3d = _safe_num(row.get("side_ret_3d"), 0.0)
    side_ret_5d = _safe_num(row.get("side_ret_5d"), 0.0)
    side_ret_10d = _safe_num(row.get("side_ret_10d"), 0.0)
    side_ret_20d = _safe_num(row.get("side_ret_20d"), 0.0)
    side_dist_ma20 = _safe_num(row.get("side_dist_ma20_atr"), 0.0)
    side_ma20_slope = _safe_num(row.get("side_ma20_slope_5"), 0.0)
    side_rsi = _safe_num(row.get("side_rsi_deviation"), 0.0)
    side_mfi = _safe_num(row.get("side_mfi_deviation"), 0.0)
    side_qqq = _safe_num(row.get("side_qqq_ret_5d"), 0.0)
    side_xlk = _safe_num(row.get("side_xlk_ret_5d"), 0.0)
    pressure = _safe_num(row.get("side_weighted_volume_pressure_5"), 0.0)
    touch_depth = _safe_num(row.get("touch_depth_atr"), 0.0)
    reentry = _safe_num(row.get("touch_reentry_signal"), 0.0)
    wick_minus_body = _safe_num(row.get("touch_wick_minus_body"), 0.0)
    close_location = _safe_num(row.get("side_close_location"), 0.0)
    consecutive = _safe_num(row.get("consecutive_touch_count"), 0.0)
    band_rank = _safe_num(row.get("band_width_percentile"), 0.5)
    bandwidth_change = _safe_num(row.get("bandwidth_change_5d"), 0.0)
    rel_volume = _safe_num(row.get("rel_volume_20"), 1.0)
    adx = _safe_num(row.get("ADX14"), 0.0)

    if playbook_id == "lower_falling_knife_continue":
        return (
            side == "Lower"
            and side_ret_5d >= 0.035
            and side_dist_ma20 >= 0.75
            and wick_minus_body < 0.25
            and (reentry <= 0.0 or consecutive >= 2)
            and pressure < 0.15
            and touch_depth < 0.55
        )

    if playbook_id == "upper_breakout_continue":
        return (
            side == "Upper"
            and side_ret_5d >= 0.035
            and (band_rank >= 0.65 or bandwidth_change >= 0.025 or adx >= 25)
            and wick_minus_body < 0.25
            and pressure > -0.15
            and (consecutive <= 3 or band_rank < 0.75)
            and side_ret_20d < 0.16
        )

    if playbook_id == "band_walk_continue":
        return (
            consecutive >= 3
            and side_ret_3d >= 0.018
            and reentry <= 0.0
            and wick_minus_body < 0.20
            and pressure < 0.35
        )

    if playbook_id == "context_trend_continue":
        return (
            side_ret_5d >= 0.025
            and side_ma20_slope >= -0.002
            and side_qqq >= 0.002
            and side_xlk >= 0.002
            and wick_minus_body < 0.25
        )

    if playbook_id == "reentry_reversal":
        return (
            reentry > 0.0
            and wick_minus_body >= 0.10
            and close_location <= 0.25
            and touch_depth >= 0.05
        )

    if playbook_id == "capitulation_reversal":
        return (
            side == "Lower"
            and consecutive >= 4
            and side_ret_5d >= 0.045
            and (pressure >= 0.20 or rel_volume >= 1.25)
            and side_qqq <= 0.015
        )

    if playbook_id == "blowoff_reversal":
        return (
            side == "Upper"
            and (side_ret_10d >= 0.07 or side_ret_20d >= 0.12)
            and side_rsi >= 0.70
            and (wick_minus_body >= 0.15 or pressure < -0.05)
            and pressure < 0.35
            and not (band_rank > 0.80 and bandwidth_change > 0.08)
        )

    if playbook_id == "stretched_reversal":
        return (
            side_dist_ma20 >= 1.75
            and touch_depth >= 0.25
            and wick_minus_body >= 0.15
            and (reentry > 0.0 or close_location <= 0.0)
            and pressure < 0.35
            and not (side == "Upper" and band_rank > 0.80 and bandwidth_change > 0.08)
            and side_mfi >= 0.25
        )

    if playbook_id == "volume_rejection_reversal":
        return wick_minus_body >= 0.28 and rel_volume >= 1.10 and close_location <= 0.35

    if playbook_id == "squeeze_release_continue":
        return (
            band_rank <= 0.35
            and side_ret_3d >= 0.018
            and touch_depth >= 0.15
            and wick_minus_body < 0.20
            and horizon <= 10
        )

    return False


def _actual_direction_for_index(feature_df: pd.DataFrame, idx: int, horizon: int) -> str | None:
    if idx + horizon >= len(feature_df):
        return None
    row = feature_df.iloc[idx]
    side = row.get("touched_side")
    if side not in ("Upper", "Lower"):
        return None
    signal_close = _safe_num(row.get("close"), np.nan)
    outcome_close = _safe_num(feature_df.iloc[idx + horizon].get("close"), np.nan)
    if not np.isfinite(signal_close) or not np.isfinite(outcome_close):
        return None
    return _actual_direction(side, signal_close, outcome_close, _continuation_hurdle_for_row(row))


def _playbook_training_labels(feature_df: pd.DataFrame, target_idx: int, horizon: int, playbook_id: str) -> list[str]:
    labels: list[str] = []
    max_idx = min(target_idx - horizon, len(feature_df) - horizon - 1)
    if max_idx < 0:
        return labels
    for idx in range(max_idx + 1):
        row = feature_df.iloc[idx]
        if _safe_bool(row.get("event_risk_blocked")):
            continue
        if not _playbook_condition(row, playbook_id, horizon):
            continue
        actual = _actual_direction_for_index(feature_df, idx, horizon)
        if actual in ("continuation", "reversal"):
            labels.append(actual)
    return labels


def _score_playbook(feature_df: pd.DataFrame, target_idx: int, horizon: int, playbook: dict) -> dict | None:
    labels = _playbook_training_labels(feature_df, target_idx, horizon, playbook["id"])
    count = len(labels)
    if count < _PLAYBOOK_MIN_MATCHES:
        return None
    direction = playbook["direction"]
    correct = sum(1 for label in labels if label == direction)
    precision = correct / count
    posterior = (correct + 1.0) / (count + 2.0)
    wilson = _wilson_lower_bound(correct, count)
    recent_labels = labels[-min(8, len(labels)) :]
    recent_correct = sum(1 for label in recent_labels if label == direction)
    recent_precision = recent_correct / len(recent_labels) if recent_labels else precision
    score = (0.50 * posterior) + (0.25 * wilson) + (0.25 * recent_precision)
    return {
        "id": playbook["id"],
        "name": playbook["name"],
        "direction": direction,
        "match_count": count,
        "correct_count": correct,
        "precision": round(precision, 6),
        "posterior_probability": round(posterior, 6),
        "wilson_lower_bound": round(wilson, 6),
        "recent_precision": round(recent_precision, 6),
        "score": round(score, 6),
    }


def _playbook_training_summary(feature_df: pd.DataFrame, target_idx: int, horizon: int) -> tuple[int, int, int]:
    labels: list[int] = []
    max_idx = min(target_idx - horizon, len(feature_df) - horizon - 1)
    if max_idx < 0:
        return 0, 0, 0
    for idx in range(max_idx + 1):
        actual = _actual_direction_for_index(feature_df, idx, horizon)
        if actual == "continuation":
            labels.append(1)
        elif actual == "reversal":
            labels.append(0)
    return len(labels), labels.count(1), labels.count(0)


def _evaluate_horizon_with_playbooks(feature_df: pd.DataFrame, row_index: int, horizon: int) -> dict:
    row = feature_df.iloc[row_index]
    training_count, continuation_count, reversal_count = _playbook_training_summary(feature_df, row_index, horizon)
    triggered = [playbook for playbook in _PLAYBOOKS if _playbook_condition(row, playbook["id"], horizon)]
    if not triggered:
        result = _no_prediction_horizon(
            horizon,
            "no_playbook_match",
            training_sample_count=training_count,
            positive_count=continuation_count,
            negative_count=reversal_count,
        )
        result["playbook_candidates"] = []
        result["model"]["type"] = "walk_forward_playbook_miner"
        return result

    scored = [
        score
        for playbook in triggered
        for score in [_score_playbook(feature_df, row_index, horizon, playbook)]
        if score is not None
    ]
    if not scored:
        result = _no_prediction_horizon(
            horizon,
            "insufficient_playbook_history",
            training_sample_count=training_count,
            positive_count=continuation_count,
            negative_count=reversal_count,
        )
        result["playbook_candidates"] = [
            {"id": playbook["id"], "name": playbook["name"], "direction": playbook["direction"]}
            for playbook in triggered
        ]
        result["model"]["type"] = "walk_forward_playbook_miner"
        return result

    scored = sorted(scored, key=lambda item: (item["score"], item["match_count"]), reverse=True)
    best = scored[0]
    enough_edge = (
        best["precision"] >= _PLAYBOOK_MIN_PRECISION
        and best["posterior_probability"] >= _PLAYBOOK_MIN_POSTERIOR
        and best["wilson_lower_bound"] >= _PLAYBOOK_MIN_WILSON
    )
    direction = best["direction"]
    continuation_probability = (
        best["posterior_probability"] if direction == "continuation" else 1.0 - best["posterior_probability"]
    )
    reversal_probability = 1.0 - continuation_probability
    confidence_score = int(round(max(continuation_probability, reversal_probability) * 100))

    if not enough_edge:
        status = "no_prediction"
        predicted_direction = None
        reason = "weak_playbook_edge"
    else:
        status = "prediction"
        predicted_direction = direction
        reason = None

    model = _model_metadata(
        training_count,
        continuation_count,
        reversal_count,
        validation_accuracy=best["precision"],
        validation_precision=best["precision"],
        candidate_count=len(scored),
    )
    model["type"] = "walk_forward_playbook_miner"
    return {
        "status": status,
        "predicted_direction": predicted_direction,
        "continuation_probability": round(continuation_probability, 6),
        "reversal_probability": round(reversal_probability, 6),
        "continuation_confidence_score": int(round(continuation_probability * 100)),
        "reversal_confidence_score": int(round(reversal_probability * 100)),
        "continuation_validation_precision": best["precision"] if direction == "continuation" else None,
        "reversal_validation_precision": best["precision"] if direction == "reversal" else None,
        "continuation_validation_count": best["match_count"] if direction == "continuation" else 0,
        "reversal_validation_count": best["match_count"] if direction == "reversal" else 0,
        "validation_policy": None,
        "confidence_score": confidence_score,
        "threshold": _PREDICTION_THRESHOLD,
        "no_prediction_reason": reason,
        "reversal_veto_reason": None,
        "analog_evidence": {
            "status": "ready",
            "direction": direction,
            "posterior_probability": best["posterior_probability"],
            "neighbor_count": best["match_count"],
            "precision": best["precision"],
            "lower_bound": best["wilson_lower_bound"],
        },
        "analog_override": False,
        "deployment_quality_gate": None,
        "blocked_prediction": None,
        "playbook": best,
        "playbook_candidates": scored[:5],
        "contributions": [
            {
                "horizon": f"{horizon}d",
                "feature": best["name"],
                "value": best["match_count"],
                "impact": direction,
                "contribution": best["score"],
            }
        ],
        "model": model,
    }


def _chart_float(value: Any) -> float | None:
    num = _safe_num(value, np.nan)
    return float(num) if np.isfinite(num) else None


def _build_entry_chart_data(feature_df: pd.DataFrame) -> list[dict]:
    if feature_df.empty:
        return []
    last_date = pd.Timestamp(feature_df.iloc[-1].get("date")).normalize()
    start_date = last_date - pd.DateOffset(days=_BACKTEST_LOOKBACK_DAYS)
    chart_df = feature_df[pd.to_datetime(feature_df["date"]) >= start_date]
    out: list[dict] = []
    for _, row in chart_df.iterrows():
        out.append(
            {
                "date": _to_date_string(row.get("date")),
                "open": _chart_float(row.get("open")),
                "high": _chart_float(row.get("high")),
                "low": _chart_float(row.get("low")),
                "close": _chart_float(row.get("close")),
                "upper": _chart_float(row.get("BB_upper")),
                "lower": _chart_float(row.get("BB_lower")),
                "isTouch": row.get("touched_side") in ("Upper", "Lower"),
            }
        )
    return out


def build_entry_decision_from_frame(
    symbol: str,
    frame: pd.DataFrame,
    *,
    as_of_date: str | pd.Timestamp | None = None,
    earnings_dates: set[pd.Timestamp] | None = None,
    earnings_symbol: str | None = None,
    context_frames: dict[str, pd.DataFrame] | None = None,
) -> dict:
    normalized = normalize_symbol(symbol)
    if not normalized:
        raise ValueError("Missing symbol for entry decision")
    if frame is None or frame.empty:
        raise ValueError(f"No data found for symbol {normalized}")

    feature_df = _prepare_feature_frame(
        frame,
        symbol=normalize_symbol(earnings_symbol or normalized),
        earnings_dates=earnings_dates,
        context_frames=context_frames,
    )
    decisions_by_index: dict[int, dict] = {}
    decisions_by_index, backtest_1y = _finalize_deployment_quality(feature_df, decisions_by_index)

    return _build_payload_from_context(
        normalized,
        as_of_date=as_of_date,
        feature_df=feature_df,
        decisions_by_index=decisions_by_index,
        backtest_1y=backtest_1y,
    )


def _load_entry_frame(symbol: str) -> tuple[str, pd.DataFrame]:
    for candidate in symbol_candidates(symbol):
        data_dict = prepare_stock_data(
            candidate,
            include_rsi=False,
            period=_TRAINING_HISTORY_PERIOD,
            interval="1d",
        )
        df = data_dict.get(candidate)
        if df is None or df.empty:
            continue
        if "close" not in df.columns:
            continue
        return candidate, df
    return "", pd.DataFrame()


def _load_context_frames(symbol: str) -> dict[str, pd.DataFrame]:
    context_frames: dict[str, pd.DataFrame] = {}
    for context_symbol in ("QQQ", "XLK"):
        try:
            data_dict = prepare_stock_data(
                context_symbol,
                include_rsi=False,
                period=_TRAINING_HISTORY_PERIOD,
                interval="1d",
            )
        except Exception:
            continue
        df = data_dict.get(context_symbol)
        if df is None or df.empty:
            continue
        context_frames[context_symbol] = df
    return context_frames


def _entry_cache_day() -> str:
    return pd.Timestamp.now(tz="America/Chicago").strftime("%Y-%m-%d")


@lru_cache(maxsize=_ENTRY_CONTEXT_CACHE_SIZE)
def _get_entry_context_cached(symbol: str, cache_day: str) -> tuple[str, pd.DataFrame, dict[int, dict], dict]:
    _ = cache_day
    resolved_symbol, frame = _load_entry_frame(symbol)
    if frame.empty:
        raise ValueError(f"No data found for symbol {symbol}")

    feature_df = _prepare_feature_frame(
        frame,
        symbol=normalize_symbol(resolved_symbol or symbol),
        earnings_dates=None,
        context_frames=_load_context_frames(resolved_symbol or symbol),
    )
    decisions_by_index: dict[int, dict] = {}
    decisions_by_index, backtest_1y = _finalize_deployment_quality(feature_df, decisions_by_index)
    latest_idx = len(feature_df) - 1
    if latest_idx not in decisions_by_index:
        decisions_by_index[latest_idx] = evaluate_row_decision(
            feature_df.iloc[latest_idx],
            feature_df=feature_df,
            row_index=latest_idx,
            force_prediction=True,
        )
        direction_gates = {
            horizon_key: result.get("direction_quality_gate", {})
            for horizon_key, result in backtest_1y.items()
            if isinstance(result, dict)
        }
        _apply_direction_quality_gates_to_decision(decisions_by_index[latest_idx], direction_gates)
        _apply_coverage_repair_policies(
            feature_df,
            {latest_idx: decisions_by_index[latest_idx]},
            _coverage_repair_policies_by_horizon(backtest_1y),
        )
    return resolved_symbol or symbol, feature_df, decisions_by_index, backtest_1y


def _build_payload_from_context(
    symbol: str,
    *,
    as_of_date: str | pd.Timestamp | None,
    feature_df: pd.DataFrame,
    decisions_by_index: dict[int, dict],
    backtest_1y: dict,
) -> dict:
    parsed_as_of_date = _parse_as_of_date(as_of_date)
    resolved_idx, date_was_snapped = _resolve_as_of_index(feature_df, parsed_as_of_date)
    selected_row = feature_df.iloc[resolved_idx]
    latest_selected = resolved_idx == len(feature_df) - 1
    selected_decision = decisions_by_index.get(resolved_idx)
    if selected_decision is None:
        selected_decision = evaluate_row_decision(
            selected_row,
            feature_df=feature_df,
            row_index=resolved_idx,
            force_prediction=latest_selected,
        )
        direction_gates = {
            horizon_key: result.get("direction_quality_gate", {})
            for horizon_key, result in backtest_1y.items()
            if isinstance(result, dict)
        }
        _apply_direction_quality_gates_to_decision(selected_decision, direction_gates)
        _apply_coverage_repair_policies(
            feature_df,
            {resolved_idx: selected_decision},
            _coverage_repair_policies_by_horizon(backtest_1y),
        )
    selected_decision = deepcopy(selected_decision)

    return {
        "symbol": symbol,
        "requested_as_of_date": _to_date_string(parsed_as_of_date),
        "as_of_date": _to_date_string(selected_row.get("date")),
        "date_was_snapped": bool(parsed_as_of_date is not None and date_was_snapped),
        "touched_side": selected_decision["touched_side"],
        "setup_type": selected_decision["setup_type"],
        "event_risk_blocked": selected_decision.get("event_risk_blocked", False),
        "prediction_threshold": _PREDICTION_THRESHOLD,
        "deployment_thresholds": {
            "continuation": _CONTINUATION_DEPLOYMENT_THRESHOLD,
            "reversal": _REVERSAL_DEPLOYMENT_THRESHOLD,
        },
        "context_status": deepcopy(feature_df.attrs.get("context_status", {})),
        "horizons": selected_decision["horizons"],
        "top_reasons": selected_decision["top_reasons"],
        "backtest_1y": deepcopy(backtest_1y),
        "chart_data": _build_entry_chart_data(feature_df),
    }


def get_entry_decision(symbol: str, as_of_date: str | None = None) -> dict:
    normalized = normalize_symbol(symbol)
    if not normalized:
        raise ValueError("Missing symbol for entry decision")

    _, feature_df, decisions_by_index, backtest_1y = _get_entry_context_cached(normalized, _entry_cache_day())

    return _build_payload_from_context(
        normalized,
        as_of_date=as_of_date,
        feature_df=feature_df,
        decisions_by_index=decisions_by_index,
        backtest_1y=backtest_1y,
    )


__all__ = [
    "build_entry_decision_from_frame",
    "evaluate_row_decision",
    "get_entry_decision",
    "run_decision_backtest",
]
