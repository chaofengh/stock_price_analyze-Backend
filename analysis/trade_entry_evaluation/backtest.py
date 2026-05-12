from __future__ import annotations

from .settings import *
from .features import *
from .model import *
from .decision import evaluate_row_decision

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
        "win_count": 0,
        "loss_count": 0,
        "win_rate": None,
        "expected_return": None,
        "expected_downside": None,
        "average_win_return": None,
        "average_loss_return": None,
        "expected_atr_return": None,
        "expected_atr_downside": None,
        "average_atr_win": None,
        "average_atr_loss": None,
        "atr_reward_risk": None,
        "confidence_buckets": [],
        "signal_tier_counts": {},
        "predictions": [],
        "recent_predictions": [],
    }


def _prediction_trade_sign(touched_side: str | None, predicted_direction: str | None) -> float:
    side_sign = _side_sign(touched_side)
    if abs(side_sign) <= _EPS or predicted_direction not in ("continuation", "reversal"):
        return 0.0
    return side_sign if predicted_direction == "continuation" else -side_sign


def _prediction_return_values(
    touched_side: str | None,
    predicted_direction: str | None,
    signal_close: float,
    outcome_close: float,
    atr: float,
) -> dict:
    trade_sign = _prediction_trade_sign(touched_side, predicted_direction)
    if abs(trade_sign) <= _EPS or not np.isfinite(signal_close) or signal_close <= 0 or not np.isfinite(outcome_close):
        return {
            "trade_direction": None,
            "trade_return": None,
            "trade_return_atr": None,
        }

    price_delta = outcome_close - signal_close
    trade_return = trade_sign * (price_delta / signal_close)
    trade_return_atr = None
    if np.isfinite(atr) and atr > 0:
        trade_return_atr = trade_sign * (price_delta / atr)
    return {
        "trade_direction": "long" if trade_sign > 0 else "short",
        "trade_return": round(float(trade_return), 6),
        "trade_return_atr": round(float(trade_return_atr), 6) if trade_return_atr is not None else None,
    }


def _mean_or_none(values: list[float]) -> float | None:
    valid = [float(value) for value in values if np.isfinite(value)]
    if not valid:
        return None
    return float(np.mean(valid))


def _round_metric(value: float | None) -> float | None:
    if value is None or not np.isfinite(value):
        return None
    return round(float(value), 6)


def _return_metrics_for_predictions(predictions: list[dict]) -> dict:
    returns = [
        _safe_num(item.get("trade_return"), np.nan)
        for item in predictions
        if np.isfinite(_safe_num(item.get("trade_return"), np.nan))
    ]
    atr_returns = [
        _safe_num(item.get("trade_return_atr"), np.nan)
        for item in predictions
        if np.isfinite(_safe_num(item.get("trade_return_atr"), np.nan))
    ]
    wins = [value for value in returns if value > 0]
    losses = [value for value in returns if value < 0]
    atr_wins = [value for value in atr_returns if value > 0]
    atr_losses = [value for value in atr_returns if value < 0]

    average_atr_win = _mean_or_none(atr_wins)
    average_atr_loss = _mean_or_none(atr_losses)
    atr_reward_risk = None
    if average_atr_win is not None and average_atr_loss is not None and abs(average_atr_loss) > _EPS:
        atr_reward_risk = average_atr_win / abs(average_atr_loss)

    return {
        "win_count": len(wins),
        "loss_count": len(losses),
        "win_rate": round(len(wins) / len(returns), 6) if returns else None,
        "expected_return": _round_metric(_mean_or_none(returns)),
        "expected_downside": _round_metric(_mean_or_none(losses) if losses else (0.0 if returns else None)),
        "average_win_return": _round_metric(_mean_or_none(wins) if wins else (0.0 if returns else None)),
        "average_loss_return": _round_metric(_mean_or_none(losses) if losses else (0.0 if returns else None)),
        "expected_atr_return": _round_metric(_mean_or_none(atr_returns)),
        "expected_atr_downside": _round_metric(_mean_or_none(atr_losses) if atr_losses else (0.0 if atr_returns else None)),
        "average_atr_win": _round_metric(average_atr_win if atr_wins else (0.0 if atr_returns else None)),
        "average_atr_loss": _round_metric(average_atr_loss if atr_losses else (0.0 if atr_returns else None)),
        "atr_reward_risk": _round_metric(atr_reward_risk),
    }


def _confidence_bucket_label(confidence_score: Any) -> tuple[int, str] | None:
    score = _safe_num(confidence_score, np.nan)
    if not np.isfinite(score):
        return None
    score_int = int(max(0, min(100, round(score))))
    if score_int >= 100:
        return 100, "100"
    lower = max(0, min(90, (score_int // 10) * 10))
    return lower, f"{lower}-{lower + 9}"


def _confidence_bucket_metrics(predictions: list[dict]) -> list[dict]:
    grouped: dict[int, dict] = {}
    for item in predictions:
        bucket = _confidence_bucket_label(item.get("confidence_score"))
        if bucket is None:
            continue
        bucket_floor, label = bucket
        group = grouped.setdefault(bucket_floor, {"bucket": label, "predictions": []})
        group["predictions"].append(item)

    out: list[dict] = []
    for bucket_floor in sorted(grouped):
        rows = grouped[bucket_floor]["predictions"]
        correct_count = sum(1 for item in rows if item.get("is_correct"))
        prediction_count = len(rows)
        metrics = _return_metrics_for_predictions(rows)
        out.append(
            {
                "bucket": grouped[bucket_floor]["bucket"],
                "bucket_floor": bucket_floor,
                "prediction_count": prediction_count,
                "correct_count": correct_count,
                "accuracy": round(correct_count / prediction_count, 6) if prediction_count else None,
                **metrics,
            }
        )
    return out


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
        atr = _safe_num(row.get("ATR14"), np.nan)
        if not np.isfinite(signal_close) or not np.isfinite(outcome_close):
            incomplete_future_count += 1
            continue

        eligible_touch_count += 1
        continuation_hurdle = _continuation_hurdle_for_row(row)
        flat_tolerance = _flat_tolerance_for_row(row)
        actual_direction = _actual_direction(touched_side, signal_close, outcome_close, flat_tolerance)
        if actual_direction == "flat":
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
        is_correct = _prediction_is_correct(predicted_direction, actual_direction)

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

        return_values = _prediction_return_values(
            touched_side,
            predicted_direction,
            signal_close,
            outcome_close,
            atr,
        )
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
                "flat_tolerance": round(flat_tolerance, 6),
                "is_correct": bool(is_correct),
                **return_values,
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
    return_metrics = _return_metrics_for_predictions(predictions)

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
        **return_metrics,
        "confidence_buckets": _confidence_bucket_metrics(predictions),
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




__all__ = [name for name in globals() if not name.startswith("__")]
