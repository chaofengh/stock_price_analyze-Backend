from __future__ import annotations

import contextlib
import logging
import os

from .settings import *

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


def _trade_direction_for_model_direction(
    touched_side: str | None,
    model_direction: str | None,
) -> str | None:
    if model_direction in ("long", "short"):
        return model_direction
    if model_direction == "flat":
        return "flat"
    if model_direction not in ("continuation", "reversal"):
        return None

    side_sign = _side_sign(touched_side)
    if abs(side_sign) <= _EPS:
        return None
    trade_sign = side_sign if model_direction == "continuation" else -side_sign
    return "long" if trade_sign > 0 else "short"


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


def _earnings_cache_day() -> str:
    return pd.Timestamp.now(tz="America/Chicago").strftime("%Y-%m-%d")


@lru_cache(maxsize=512)
def _cached_earnings_dates(symbol: str, cache_day: str | None = None) -> tuple[pd.Timestamp, ...]:
    _ = cache_day
    try:
        ticker = yf.Ticker(symbol)
        if not hasattr(ticker, "get_earnings_dates"):
            return tuple()
        with _quiet_yfinance_earnings_lookup():
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

    cached = _cached_earnings_dates(symbol, _earnings_cache_day())
    if not cached:
        return set()

    low = min_date.normalize() - pd.Timedelta(days=_EARNINGS_DATE_LOOKBACK_CALENDAR_DAYS)
    high = max_date.normalize() + pd.Timedelta(days=_EARNINGS_DATE_LOOKAHEAD_CALENDAR_DAYS)
    return {d for d in cached if low <= d <= high}


@contextlib.contextmanager
def _quiet_yfinance_earnings_lookup():
    yf_logger = logging.getLogger("yfinance")
    old_disabled = yf_logger.disabled
    old_level = yf_logger.level
    try:
        yf_logger.disabled = True
        yf_logger.setLevel(logging.CRITICAL)
        with open(os.devnull, "w") as devnull:
            with contextlib.redirect_stdout(devnull), contextlib.redirect_stderr(devnull):
                yield
    finally:
        yf_logger.disabled = old_disabled
        yf_logger.setLevel(old_level)


def _event_risk_window_details(dates: pd.Series, earnings_dates: set[pd.Timestamp]) -> pd.DataFrame:
    details = pd.DataFrame(
        {
            "event_risk_blocked": pd.Series(False, index=dates.index, dtype=bool),
            "event_risk_event_date": pd.Series(None, index=dates.index, dtype=object),
            "event_risk_sessions_to_event": pd.Series(pd.NA, index=dates.index, dtype="Int64"),
            "event_risk_calendar_days_to_event": pd.Series(pd.NA, index=dates.index, dtype="Int64"),
            "event_risk_reason": pd.Series(None, index=dates.index, dtype=object),
        }
    )
    if dates.empty or not earnings_dates:
        return details

    base = pd.DatetimeIndex(pd.to_datetime(dates, errors="coerce")).normalize()
    for event_date in sorted(earnings_dates):
        event_day = pd.Timestamp(event_date).normalize()
        event_date_value = event_day.date()
        for idx, row_day in enumerate(base):
            if pd.isna(row_day):
                continue
            row_date_value = row_day.date()
            sessions_to_event: int | None = None
            reason: str | None = None
            if row_day <= event_day:
                sessions_to_event = np.busday_count(row_date_value, event_date_value)
                if sessions_to_event <= _EARNINGS_BLACKOUT_PRE_EVENT_SESSIONS:
                    reason = "earnings_within_prediction_window"
            else:
                sessions_since_event = np.busday_count(event_date_value, row_date_value)
                if sessions_since_event <= _EARNINGS_BLACKOUT_POST_EVENT_SESSIONS:
                    sessions_to_event = -int(sessions_since_event)
                    reason = "earnings_cooldown"

            if reason is None:
                continue

            current_sessions = details.iloc[idx]["event_risk_sessions_to_event"]
            should_replace = pd.isna(current_sessions) or abs(int(sessions_to_event)) < abs(int(current_sessions))
            if not should_replace:
                continue

            details.iloc[idx, details.columns.get_loc("event_risk_blocked")] = True
            details.iloc[idx, details.columns.get_loc("event_risk_event_date")] = _to_date_string(event_day)
            details.iloc[idx, details.columns.get_loc("event_risk_sessions_to_event")] = int(sessions_to_event)
            details.iloc[idx, details.columns.get_loc("event_risk_calendar_days_to_event")] = int(
                (event_day - row_day).days
            )
            details.iloc[idx, details.columns.get_loc("event_risk_reason")] = reason

    return details


def _mark_event_risk_window(dates: pd.Series, earnings_dates: set[pd.Timestamp]) -> pd.Series:
    return _event_risk_window_details(dates, earnings_dates)["event_risk_blocked"]


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
    event_risk_details = _event_risk_window_details(df["date"], earnings_set)
    for col in event_risk_details.columns:
        df[col] = event_risk_details[col].to_numpy()
    df.attrs["context_status"] = context_status

    return df


def _actual_direction(
    touched_side: str,
    signal_close: float,
    outcome_close: float,
    flat_tolerance: float = 0.0,
) -> str:
    side_sign = _side_sign(touched_side)
    if abs(side_sign) <= _EPS:
        return "flat"

    tolerance = max(0.0, _safe_num(flat_tolerance, 0.0), _FLAT_PRICE_ABSOLUTE_TOLERANCE)
    price_delta = outcome_close - signal_close
    if abs(price_delta) <= tolerance:
        return "flat"

    continuation_move = side_sign * price_delta
    return "continuation" if continuation_move > 0 else "reversal"


def _flat_tolerance_for_row(row: pd.Series) -> float:
    return _FLAT_PRICE_ABSOLUTE_TOLERANCE


def _prediction_is_correct(predicted_direction: str | None, actual_direction: str | None) -> bool:
    if predicted_direction not in ("continuation", "reversal"):
        return False
    if actual_direction in ("continuation", "reversal"):
        return predicted_direction == actual_direction
    return actual_direction == "flat" and predicted_direction == "reversal"


def _continuation_hurdle_for_row(row: pd.Series) -> float:
    atr = _safe_num(row.get("ATR14"), np.nan)
    if not np.isfinite(atr) or atr <= 0:
        return 0.0
    return _DIRECTION_ATR_THRESHOLD * atr


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




__all__ = [name for name in globals() if not name.startswith("__")]
