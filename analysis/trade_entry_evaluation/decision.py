from __future__ import annotations

from .settings import *
from .features import *
from .model import *
from .adaptive import _evaluate_horizon_with_adaptive_analogs

def _evaluate_horizon_from_context(feature_df: pd.DataFrame, row_index: int, horizon: int) -> dict:
    return _evaluate_horizon_with_adaptive_analogs(feature_df, row_index, horizon)


def _event_risk_summary(row: pd.Series) -> dict:
    sessions_to_event = row.get("event_risk_sessions_to_event")
    calendar_days_to_event = row.get("event_risk_calendar_days_to_event")
    reason = row.get("event_risk_reason")
    blocked = _safe_bool(row.get("event_risk_blocked"))
    return {
        "blocked": blocked,
        "within_model_window": blocked,
        "event_date": _to_date_string(row.get("event_risk_event_date")),
        "sessions_to_event": (
            int(sessions_to_event)
            if not pd.isna(sessions_to_event)
            else None
        ),
        "calendar_days_to_event": (
            int(calendar_days_to_event)
            if not pd.isna(calendar_days_to_event)
            else None
        ),
        "reason": None if pd.isna(reason) else reason,
        "pre_event_blackout_sessions": _EARNINGS_BLACKOUT_PRE_EVENT_SESSIONS,
        "post_event_blackout_sessions": _EARNINGS_BLACKOUT_POST_EVENT_SESSIONS,
    }


def _event_risk_blocks_horizon(event_risk: dict, horizon: int) -> bool:
    if not event_risk.get("within_model_window") and not event_risk.get("blocked"):
        return False

    sessions_to_event = event_risk.get("sessions_to_event")
    if sessions_to_event is None:
        return True

    sessions_to_event = int(sessions_to_event)
    if sessions_to_event < 0:
        return abs(sessions_to_event) <= _EARNINGS_BLACKOUT_POST_EVENT_SESSIONS

    return sessions_to_event <= horizon


def _event_risk_for_horizon(event_risk: dict, horizon: int) -> dict:
    horizon_risk = deepcopy(event_risk)
    horizon_risk["horizon_days"] = horizon
    horizon_risk["blocked"] = _event_risk_blocks_horizon(event_risk, horizon)
    return horizon_risk


def evaluate_row_decision(
    row: pd.Series,
    *,
    feature_df: pd.DataFrame | None = None,
    row_index: int | None = None,
    force_prediction: bool = False,
) -> dict:
    touched_side = row.get("touched_side")
    touched = touched_side in ("Upper", "Lower")
    event_risk = _event_risk_summary(row)

    if not touched and not force_prediction:
        horizons = {f"{h}d": _no_prediction_horizon(h, "no_bollinger_touch") for h in _HORIZONS}
        event_risk["blocked_horizons"] = []
        event_risk["blocked"] = False
        return {
            "touched_side": touched_side,
            "setup_type": "no_band_setup",
            "event_risk_blocked": False,
            "event_risk": event_risk,
            "horizons": horizons,
            "top_reasons": [],
        }

    horizons = {}
    blocked_horizons = []
    for h in _HORIZONS:
        horizon_key = f"{h}d"
        horizon_event_risk = _event_risk_for_horizon(event_risk, h)
        if horizon_event_risk["blocked"]:
            horizon = _no_prediction_horizon(h, "event_risk")
            horizon["event_risk"] = horizon_event_risk
            blocked_horizons.append(horizon_key)
        elif feature_df is None or row_index is None:
            horizon = _no_prediction_horizon(h, "insufficient_training_data")
            horizon["event_risk"] = horizon_event_risk
        else:
            horizon = _evaluate_horizon_from_context(feature_df, row_index, h)
            horizon["event_risk"] = horizon_event_risk
        horizons[horizon_key] = horizon

    event_risk["blocked_horizons"] = blocked_horizons
    event_risk["blocked"] = bool(blocked_horizons)
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
        "event_risk_blocked": bool(blocked_horizons),
        "event_risk": event_risk,
        "horizons": horizons,
        "top_reasons": top_reasons,
    }


def _build_decisions_by_index(feature_df: pd.DataFrame) -> dict[int, dict]:
    decisions: dict[int, dict] = {}
    for idx in range(len(feature_df)):
        row = feature_df.iloc[idx]
        if row.get("touched_side") not in ("Upper", "Lower"):
            continue
        decisions[idx] = evaluate_row_decision(row, feature_df=feature_df, row_index=idx)
    return decisions




__all__ = [name for name in globals() if not name.startswith("__")]
