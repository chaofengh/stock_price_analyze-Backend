from __future__ import annotations

from .settings import *
from .features import *
from .model import *
from .adaptive import _evaluate_horizon_with_adaptive_analogs

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




__all__ = [name for name in globals() if not name.startswith("__")]
