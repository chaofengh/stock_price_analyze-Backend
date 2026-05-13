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

from ..data_preparation import prepare_stock_data
from ..data_fetcher_utils import normalize_symbol, symbol_candidates

_EPS = 1e-9
_PREDICTION_THRESHOLD = 0.75
_CONTINUATION_DEPLOYMENT_THRESHOLD = 0.85
_REVERSAL_DEPLOYMENT_THRESHOLD = 0.85
_ENTRY_CONTEXT_CACHE_SIZE = 128
_HORIZONS = (5, 10)
_EARNINGS_BLACKOUT_PRE_EVENT_SESSIONS = max(_HORIZONS)
_EARNINGS_BLACKOUT_POST_EVENT_SESSIONS = 1
_EARNINGS_DATE_LOOKAHEAD_CALENDAR_DAYS = max(_HORIZONS) * 3
_EARNINGS_DATE_LOOKBACK_CALENDAR_DAYS = 7
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
_COVERAGE_EXPANSION_MAX_ACCURACY_DROP = 0.025
_MAX_EXACT_COVERAGE_EXPANSION_CANDIDATES = 18
_COVERAGE_POLICY_MAX_SAFE = "max_safe_accuracy_preserving"
_FLAT_PRICE_ABSOLUTE_TOLERANCE = 0.005
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




__all__ = [name for name in globals() if not name.startswith("__")]
