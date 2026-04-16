"""
trade_entry_evaluation.py
Purpose: deterministic 2-stage decision layer on top of Bollinger touches
and a matching 1-year next-day prediction-accuracy evaluator.
"""
from __future__ import annotations

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
_STAGE_A_THRESHOLD = 0.55
_STAGE_B_THRESHOLD = 0.58


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

        # closest trading bar to the event date
        chosen = min(candidates, key=lambda idx: abs((base[idx] - event_date).days))
        start = max(0, chosen - 1)
        end = min(len(base) - 1, chosen + 1)
        flags.iloc[start : end + 1] = True

    return flags


def _prepare_feature_frame(
    frame: pd.DataFrame,
    *,
    symbol: str,
    earnings_dates: set[pd.Timestamp] | None = None,
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

    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    open_ = df["open"].astype(float)
    volume = df["volume"].fillna(0.0).astype(float)

    df["ATR14"] = talib.ATR(high.values, low.values, close.values, timeperiod=14)
    df["ADX14"] = talib.ADX(high.values, low.values, close.values, timeperiod=14)
    df["MA20"] = talib.SMA(close.values, timeperiod=20)
    df["MA50"] = talib.SMA(close.values, timeperiod=50)

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
    df["upper_wick_ratio"] = upper_wick / (candle_range + _EPS)
    df["lower_wick_ratio"] = lower_wick / (candle_range + _EPS)
    df["close_in_range"] = ((close - low) / (candle_range + _EPS)).clip(0.0, 1.0)

    df["gap"] = open_ - prev_close
    df["gap_atr"] = df["gap"] / (df["ATR14"] + _EPS)

    df["inside_bar"] = ((high <= high.shift(1)) & (low >= low.shift(1))).astype(float)
    df["outside_bar"] = ((high >= high.shift(1)) & (low <= low.shift(1))).astype(float)

    df["true_range"] = true_range
    df["true_range_atr"] = true_range / (df["ATR14"] + _EPS)
    df["range_expansion_5"] = candle_range / (candle_range.shift(1).rolling(5, min_periods=2).mean() + _EPS)

    df["band_width"] = (df["BB_upper"] - df["BB_lower"]) / (df["BB_middle"].abs() + _EPS)
    df["pct_b"] = (close - df["BB_lower"]) / (df["BB_upper"] - df["BB_lower"] + _EPS)
    df["distance_to_middle_atr"] = (df["BB_middle"] - close) / (df["ATR14"] + _EPS)
    df["bandwidth_change_1d"] = df["band_width"].diff(1)
    df["bandwidth_change_3d"] = df["band_width"].diff(3)
    df["bandwidth_change_5d"] = df["band_width"].diff(5)

    returns = close.pct_change()
    df["ret_1d"] = returns
    df["ret_5d"] = close.pct_change(5)
    df["realized_vol_20"] = returns.rolling(20, min_periods=10).std() * math.sqrt(252)

    df["rel_volume_20"] = volume / (volume.rolling(20, min_periods=5).mean() + _EPS)
    vol_mean = volume.rolling(20, min_periods=5).mean()
    vol_std = volume.rolling(20, min_periods=5).std()
    df["volume_zscore_20"] = (volume - vol_mean) / (vol_std + _EPS)
    df["dollar_volume"] = close * volume

    obv = talib.OBV(close.values.astype(float), volume.values.astype(float))
    df["obv"] = obv
    df["obv_slope_5"] = (df["obv"] - df["obv"].shift(5)) / (df["obv"].shift(5).abs() + _EPS)

    up_volume = np.where(close > prev_close, volume, 0.0)
    down_volume = np.where(close < prev_close, volume, 0.0)
    up_roll = pd.Series(up_volume, index=df.index).rolling(5, min_periods=2).sum()
    down_roll = pd.Series(down_volume, index=df.index).rolling(5, min_periods=2).sum()
    df["up_down_volume_ratio_5"] = up_roll / (down_roll + _EPS)
    df["volume_range_interaction"] = df["rel_volume_20"] * df["true_range_atr"]

    df["ma20_slope_5"] = (df["MA20"] - df["MA20"].shift(5)) / (df["MA20"].shift(5).abs() + _EPS)
    df["ma50_slope_5"] = (df["MA50"] - df["MA50"].shift(5)) / (df["MA50"].shift(5).abs() + _EPS)
    df["dist_ma20_atr"] = (close - df["MA20"]) / (df["ATR14"] + _EPS)
    df["dist_ma50_atr"] = (close - df["MA50"]) / (df["ATR14"] + _EPS)

    streak = _compute_signed_streak(returns)
    df["signed_streak"] = streak
    df["directional_streak"] = (streak / 5.0).clip(-1.0, 1.0)

    df["band_width_percentile"] = df["band_width"].rank(method="average", pct=True)
    df["realized_vol_percentile"] = df["realized_vol_20"].rank(method="average", pct=True)

    df["touched_side"] = df.apply(_touch_side_for_row, axis=1)
    df["consecutive_touch_count"] = _consecutive_touch_counts(df["touched_side"])

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

    df["target_distance_atr"] = (df["BB_middle"] - close).abs() / (df["ATR14"] + _EPS)

    if df.empty:
        raise ValueError("No data available for decision evaluation.")

    min_date = pd.Timestamp(df["date"].iloc[0])
    max_date = pd.Timestamp(df["date"].iloc[-1])
    earnings_set = _resolve_earnings_dates(symbol, min_date=min_date, max_date=max_date, earnings_dates=earnings_dates)
    df["event_risk_blocked"] = _mark_event_risk_window(df["date"], earnings_set)

    return df


def _component(
    *,
    stage: str,
    feature: str,
    value: Any,
    raw: float,
    weight: float,
    positive_impact: str,
    negative_impact: str,
) -> dict:
    bounded_raw = _clamp(_safe_num(raw), -1.0, 1.0)
    contribution = weight * bounded_raw
    return {
        "stage": stage,
        "feature": feature,
        "value": _value_for_payload(value),
        "impact": positive_impact if contribution >= 0 else negative_impact,
        "contribution": round(contribution, 6),
    }


def _classify_setup(touched_side: str | None, enter_today: bool) -> str:
    if touched_side is None:
        return "no_band_setup"
    if enter_today and touched_side == "Upper":
        return "upper_band_mean_reversion"
    if enter_today and touched_side == "Lower":
        return "lower_band_mean_reversion"
    return "trend_continuation_trap"


def evaluate_row_decision(
    row: pd.Series,
    *,
    stage_a_threshold: float = _STAGE_A_THRESHOLD,
    stage_b_threshold: float = _STAGE_B_THRESHOLD,
) -> dict:
    touched_side = row.get("touched_side")
    touched = touched_side in ("Upper", "Lower")

    stage_a_contrib: list[dict] = []
    stage_b_contrib: list[dict] = []

    if touched:
        wick_ratio = row.get("upper_wick_ratio") if touched_side == "Upper" else row.get("lower_wick_ratio")
        stage_a_components = [
            _component(
                stage="stage_a",
                feature="touch_reentry_signal",
                value=row.get("touch_reentry_signal"),
                raw=row.get("touch_reentry_signal"),
                weight=1.15,
                positive_impact="mean_reversion_favorable",
                negative_impact="trend_continuation",
            ),
            _component(
                stage="stage_a",
                feature="adx_regime",
                value=row.get("ADX14"),
                raw=(22.0 - _safe_num(row.get("ADX14"))) / 14.0,
                weight=0.95,
                positive_impact="mean_reversion_favorable",
                negative_impact="trend_continuation",
            ),
            _component(
                stage="stage_a",
                feature="trend_alignment",
                value=row.get("trend_alignment"),
                raw=_safe_num(row.get("trend_alignment")) / 2.0,
                weight=0.90,
                positive_impact="mean_reversion_favorable",
                negative_impact="trend_continuation",
            ),
            _component(
                stage="stage_a",
                feature="consecutive_touch_count",
                value=row.get("consecutive_touch_count"),
                raw=(2.0 - _safe_num(row.get("consecutive_touch_count"))) / 3.0,
                weight=0.80,
                positive_impact="mean_reversion_favorable",
                negative_impact="trend_continuation",
            ),
            _component(
                stage="stage_a",
                feature="wick_rejection",
                value=wick_ratio,
                raw=_safe_num(wick_ratio) - _safe_num(row.get("body_pct")),
                weight=0.85,
                positive_impact="mean_reversion_favorable",
                negative_impact="trend_continuation",
            ),
            _component(
                stage="stage_a",
                feature="volume_range_interaction",
                value=row.get("volume_range_interaction"),
                raw=1.10 - _safe_num(row.get("volume_range_interaction"), 1.10),
                weight=0.70,
                positive_impact="mean_reversion_favorable",
                negative_impact="trend_continuation",
            ),
            _component(
                stage="stage_a",
                feature="bandwidth_change_3d",
                value=row.get("bandwidth_change_3d"),
                raw=-8.0 * _safe_num(row.get("bandwidth_change_3d")),
                weight=0.55,
                positive_impact="mean_reversion_favorable",
                negative_impact="trend_continuation",
            ),
            _component(
                stage="stage_a",
                feature="realized_vol_percentile",
                value=row.get("realized_vol_percentile"),
                raw=0.70 - _safe_num(row.get("realized_vol_percentile"), 0.70),
                weight=0.45,
                positive_impact="mean_reversion_favorable",
                negative_impact="trend_continuation",
            ),
        ]
        stage_a_contrib.extend(stage_a_components)

    stage_a_score = float(sum(c["contribution"] for c in stage_a_contrib))
    stage_a_probability = _sigmoid(stage_a_score) if touched else 0.5

    event_risk_blocked = _safe_bool(row.get("event_risk_blocked"))
    if event_risk_blocked:
        stage_a_contrib.append(
            {
                "stage": "stage_a",
                "feature": "earnings_event_window",
                "value": True,
                "impact": "trend_continuation",
                "contribution": -1.5,
            }
        )

    stage_a_favorable = bool(touched and (not event_risk_blocked) and stage_a_probability >= stage_a_threshold)

    if touched:
        direction_sign = -1.0 if touched_side == "Upper" else 1.0
        wick_ratio = row.get("upper_wick_ratio") if touched_side == "Upper" else row.get("lower_wick_ratio")

        close_location_raw = (
            (0.5 - _safe_num(row.get("close_in_range"))) * 2.0
            if touched_side == "Upper"
            else (_safe_num(row.get("close_in_range")) - 0.5) * 2.0
        )
        toward_touch_streak = (
            _safe_num(row.get("directional_streak"))
            if touched_side == "Upper"
            else -_safe_num(row.get("directional_streak"))
        )

        stage_b_components = [
            _component(
                stage="stage_b",
                feature="wick_rejection",
                value=wick_ratio,
                raw=_safe_num(wick_ratio) - _safe_num(row.get("body_pct")),
                weight=1.05,
                positive_impact="good_entry",
                negative_impact="bad_entry",
            ),
            _component(
                stage="stage_b",
                feature="close_location_in_bar",
                value=row.get("close_in_range"),
                raw=close_location_raw,
                weight=0.95,
                positive_impact="good_entry",
                negative_impact="bad_entry",
            ),
            _component(
                stage="stage_b",
                feature="body_pct",
                value=row.get("body_pct"),
                raw=0.60 - _safe_num(row.get("body_pct"), 0.60),
                weight=0.70,
                positive_impact="good_entry",
                negative_impact="bad_entry",
            ),
            _component(
                stage="stage_b",
                feature="rel_volume_20",
                value=row.get("rel_volume_20"),
                raw=1.25 - _safe_num(row.get("rel_volume_20"), 1.25),
                weight=0.70,
                positive_impact="good_entry",
                negative_impact="bad_entry",
            ),
            _component(
                stage="stage_b",
                feature="gap_atr",
                value=row.get("gap_atr"),
                raw=direction_sign * _safe_num(row.get("gap_atr")),
                weight=0.65,
                positive_impact="good_entry",
                negative_impact="bad_entry",
            ),
            _component(
                stage="stage_b",
                feature="toward_touch_streak",
                value=row.get("directional_streak"),
                raw=-toward_touch_streak,
                weight=0.80,
                positive_impact="good_entry",
                negative_impact="bad_entry",
            ),
            _component(
                stage="stage_b",
                feature="target_distance_atr",
                value=row.get("target_distance_atr"),
                raw=(_safe_num(row.get("target_distance_atr")) - 0.35) / 1.25,
                weight=0.90,
                positive_impact="good_entry",
                negative_impact="bad_entry",
            ),
            _component(
                stage="stage_b",
                feature="volume_zscore_20",
                value=row.get("volume_zscore_20"),
                raw=-_safe_num(row.get("volume_zscore_20")) / 2.0,
                weight=0.55,
                positive_impact="good_entry",
                negative_impact="bad_entry",
            ),
        ]
        stage_b_contrib.extend(stage_b_components)

    entry_score = float(sum(c["contribution"] for c in stage_b_contrib))
    reversion_probability = _sigmoid(entry_score) if touched else 0.5
    continuation_probability = 1.0 - reversion_probability

    enter_today = bool(stage_a_favorable and reversion_probability >= stage_b_threshold)
    setup_type = _classify_setup(touched_side, enter_today)

    target_distance_atr = _safe_num(row.get("target_distance_atr"), np.nan)
    expected_return_to_target_atr = (
        round(target_distance_atr, 4) if touched and np.isfinite(target_distance_atr) else None
    )

    if not touched:
        expected_adverse_move_atr = None
    elif enter_today:
        expected_adverse_move_atr = 1.0
    else:
        expected_adverse_move_atr = round(1.0 + 0.5 * continuation_probability, 4)

    stage_strength = abs(stage_a_probability - 0.5) * 2.0
    entry_strength = abs(reversion_probability - 0.5) * 2.0
    confidence_score = int(round(_clamp((0.45 * stage_strength + 0.55 * entry_strength) * 100.0, 0.0, 100.0)))
    if not touched:
        confidence_score = 0

    combined = stage_a_contrib + stage_b_contrib
    top_reasons = sorted(combined, key=lambda item: abs(_safe_num(item.get("contribution"))), reverse=True)[:5]

    return {
        "touched_side": touched_side,
        "setup_type": setup_type,
        "enter_today": enter_today,
        "reversion_probability": round(reversion_probability, 6),
        "continuation_probability": round(continuation_probability, 6),
        "expected_return_to_target_atr": expected_return_to_target_atr,
        "expected_adverse_move_atr": expected_adverse_move_atr,
        "confidence_score": confidence_score,
        "stage_a": {
            "is_favorable": stage_a_favorable,
            "probability": round(stage_a_probability, 6),
            "event_risk_blocked": event_risk_blocked,
            "threshold": stage_a_threshold,
            "contributions": stage_a_contrib,
        },
        "stage_b": {
            "entry_probability": round(reversion_probability, 6),
            "threshold": stage_b_threshold,
            "contributions": stage_b_contrib,
        },
        "top_reasons": top_reasons,
    }


def run_decision_backtest(
    feature_df: pd.DataFrame,
    decisions_by_index: dict[int, dict] | None = None,
) -> dict:
    if feature_df.empty:
        return {
            "period_start": None,
            "period_end": None,
            "sample_count": 0,
            "correct_count": 0,
            "accuracy": None,
            "reverse_call_count": 0,
            "continue_call_count": 0,
            "reverse_precision": None,
            "continue_precision": None,
            "tp_reverse": 0,
            "fp_reverse": 0,
            "tn_reverse": 0,
            "fn_reverse": 0,
            "flat_count": 0,
            "recent_predictions": [],
        }

    sample_count = 0
    correct_count = 0
    reverse_call_count = 0
    continue_call_count = 0
    reverse_correct_count = 0
    continue_correct_count = 0
    tp_reverse = 0
    fp_reverse = 0
    tn_reverse = 0
    fn_reverse = 0
    flat_count = 0
    predictions: list[dict] = []

    for idx in range(len(feature_df) - 1):
        row = feature_df.iloc[idx]
        touched_side = row.get("touched_side")
        if touched_side not in ("Upper", "Lower"):
            continue

        decision = (
            decisions_by_index[idx]
            if decisions_by_index is not None and idx in decisions_by_index
            else evaluate_row_decision(row)
        )
        predicted_reverse = _safe_bool(decision.get("enter_today"))
        predicted_class = "reverse" if predicted_reverse else "continue"

        next_row = feature_df.iloc[idx + 1]
        signal_close = _safe_num(row.get("close"), np.nan)
        next_close = _safe_num(next_row.get("close"), np.nan)
        if not np.isfinite(signal_close) or not np.isfinite(next_close):
            continue

        sample_count += 1
        if predicted_reverse:
            reverse_call_count += 1
        else:
            continue_call_count += 1

        if touched_side == "Upper":
            truth_reverse = next_close < signal_close
            truth_continue = next_close > signal_close
        else:
            truth_reverse = next_close > signal_close
            truth_continue = next_close < signal_close

        if np.isclose(next_close, signal_close, atol=1e-12, rtol=0.0):
            flat_count += 1
            truth_reverse = False
            truth_continue = False

        is_correct = (predicted_reverse and truth_reverse) or ((not predicted_reverse) and truth_continue)
        if is_correct:
            correct_count += 1

        if truth_reverse:
            if predicted_reverse:
                tp_reverse += 1
            else:
                fn_reverse += 1
        elif truth_continue:
            if predicted_reverse:
                fp_reverse += 1
            else:
                tn_reverse += 1
        else:
            # Flat next-day close is always treated as incorrect.
            if predicted_reverse:
                fp_reverse += 1
            else:
                fn_reverse += 1

        if predicted_reverse and is_correct:
            reverse_correct_count += 1
        if (not predicted_reverse) and is_correct:
            continue_correct_count += 1

        change_pct = ((next_close - signal_close) / signal_close) * 100.0 if signal_close else 0.0
        actual_next_day_direction = (
            "reverse" if truth_reverse else ("continue" if truth_continue else "flat")
        )

        predictions.append(
            {
                "signal_date": _to_date_string(row.get("date")),
                "next_date": _to_date_string(next_row.get("date")),
                "touched_side": touched_side,
                "predicted_class": predicted_class,
                "actual_next_day_direction": actual_next_day_direction,
                "signal_close": round(signal_close, 6),
                "next_close": round(next_close, 6),
                "next_day_change_pct": round(change_pct, 6),
                "is_correct": is_correct,
                "stage_a_probability": decision.get("stage_a", {}).get("probability"),
                "reversion_probability": decision.get("reversion_probability"),
            }
        )

    if sample_count == 0:
        return {
            "period_start": _to_date_string(feature_df.iloc[0].get("date")),
            "period_end": _to_date_string(feature_df.iloc[-1].get("date")),
            "sample_count": 0,
            "correct_count": 0,
            "accuracy": None,
            "reverse_call_count": 0,
            "continue_call_count": 0,
            "reverse_precision": None,
            "continue_precision": None,
            "tp_reverse": 0,
            "fp_reverse": 0,
            "tn_reverse": 0,
            "fn_reverse": 0,
            "flat_count": 0,
            "recent_predictions": [],
        }

    accuracy = correct_count / sample_count
    reverse_precision = (
        reverse_correct_count / reverse_call_count if reverse_call_count > 0 else None
    )
    continue_precision = (
        continue_correct_count / continue_call_count if continue_call_count > 0 else None
    )

    return {
        "period_start": _to_date_string(feature_df.iloc[0].get("date")),
        "period_end": _to_date_string(feature_df.iloc[-1].get("date")),
        "sample_count": sample_count,
        "correct_count": correct_count,
        "accuracy": round(accuracy, 6),
        "reverse_call_count": reverse_call_count,
        "continue_call_count": continue_call_count,
        "reverse_precision": round(reverse_precision, 6) if reverse_precision is not None else None,
        "continue_precision": round(continue_precision, 6) if continue_precision is not None else None,
        "tp_reverse": tp_reverse,
        "fp_reverse": fp_reverse,
        "tn_reverse": tn_reverse,
        "fn_reverse": fn_reverse,
        "flat_count": flat_count,
        "recent_predictions": list(reversed(predictions[-20:])),
    }


def build_entry_decision_from_frame(
    symbol: str,
    frame: pd.DataFrame,
    *,
    as_of_date: str | pd.Timestamp | None = None,
    earnings_dates: set[pd.Timestamp] | None = None,
    earnings_symbol: str | None = None,
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
    )
    parsed_as_of_date = _parse_as_of_date(as_of_date)
    resolved_idx, date_was_snapped = _resolve_as_of_index(feature_df, parsed_as_of_date)
    decision_cache: dict[int, dict] = {}
    selected_row = feature_df.iloc[resolved_idx]
    selected_decision = evaluate_row_decision(selected_row)
    decision_cache[resolved_idx] = selected_decision

    payload = {
        "symbol": normalized,
        "requested_as_of_date": _to_date_string(parsed_as_of_date),
        "as_of_date": _to_date_string(selected_row.get("date")),
        "date_was_snapped": bool(parsed_as_of_date is not None and date_was_snapped),
        "touched_side": selected_decision["touched_side"],
        "setup_type": selected_decision["setup_type"],
        "enter_today": selected_decision["enter_today"],
        "reversion_probability": selected_decision["reversion_probability"],
        "continuation_probability": selected_decision["continuation_probability"],
        "expected_return_to_target_atr": selected_decision["expected_return_to_target_atr"],
        "expected_adverse_move_atr": selected_decision["expected_adverse_move_atr"],
        "confidence_score": selected_decision["confidence_score"],
        "stage_a": selected_decision["stage_a"],
        "stage_b": selected_decision["stage_b"],
        "top_reasons": selected_decision["top_reasons"],
        "backtest_1y": run_decision_backtest(feature_df, decisions_by_index=decision_cache),
    }

    return payload


def _load_entry_frame(symbol: str) -> tuple[str, pd.DataFrame]:
    for candidate in symbol_candidates(symbol):
        data_dict = prepare_stock_data(
            candidate,
            include_rsi=False,
            period="1y",
            interval="1d",
        )
        df = data_dict.get(candidate)
        if df is None or df.empty:
            continue
        if "close" not in df.columns:
            continue
        return candidate, df
    return "", pd.DataFrame()


def get_entry_decision(symbol: str, as_of_date: str | None = None) -> dict:
    normalized = normalize_symbol(symbol)
    if not normalized:
        raise ValueError("Missing symbol for entry decision")

    resolved_symbol, frame = _load_entry_frame(normalized)
    if frame.empty:
        raise ValueError(f"No data found for symbol {normalized}")

    return build_entry_decision_from_frame(
        normalized,
        frame,
        as_of_date=as_of_date,
        earnings_dates=None,
        earnings_symbol=resolved_symbol or normalized,
    )


__all__ = [
    "build_entry_decision_from_frame",
    "evaluate_row_decision",
    "get_entry_decision",
    "run_decision_backtest",
]
