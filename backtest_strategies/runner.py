
from concurrent.futures import as_completed
import multiprocessing, datetime, pandas as pd
from joblib import Parallel, delayed, parallel_backend

from .data_fetcher import fetch_intraday_data
from .param_grid import generate as param_grid
from .metrics import compute_metrics
from analysis.indicators import (compute_bollinger_bands, compute_realtime_sr)
from .logging_config import logger
from .orb_strategies import (
    compute_atr,
    compute_intraday_vwap,
    backtest_orb,
    backtest_reverse_orb,
)
from .bb_sr_strategies import (
    backtest_bbands,
    backtest_support_resistance,
)

# ────────────────────────────────────────────────────────────────
# Pre‑processing helpers
# ────────────────────────────────────────────────────────────────
def _preprocess(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["atr"]   = compute_atr(df)
    df["vwap"]  = compute_intraday_vwap(df)
    df = compute_bollinger_bands(df)
    df = compute_realtime_sr(df)
    df["date_utc"] = df.index.strftime("%Y-%m-%d")
    return df.dropna(subset=["atr", "vwap", "BB_upper", "BB_lower"])


def _build_or_lookup(df: pd.DataFrame, or_minutes_list) -> dict:
    """
    or_levels[m][day] = (or_high, or_low)
    """
    lookup = {m: {} for m in or_minutes_list}
    for day_str, day_df in df.groupby("date_utc"):
        session = day_df.between_time("14:30", "21:00")
        if session.empty:
            continue
        bar_min = int((session.index[1] - session.index[0]).seconds / 60)
        for m in or_minutes_list:
            or_bars = max(1, m // bar_min)
            block   = session.iloc[:or_bars]
            lookup[m][day_str] = (block["high"].max(), block["low"].min())
    return lookup


# ────────────────────────────────────────────────────────────────
# Strategy wrappers
# ────────────────────────────────────────────────────────────────
SINGLE_PASS_DEFAULTS = {
    "open_range_minutes"   : 0,        # dummy – ignored by the strat itself
    "time_exit_minutes"    : None,
    "max_entries"          : 1,
    "use_volume_filter"    : False,
    "use_vwap_filter"      : False,
    "stop_loss"            : None,
    "atr_stop_multiplier"  : None,
    "limit_same_direction" : False,
}

def _run_one(df, func, or_levels, **p):
    """Full run – returns metrics + daily_trades"""
    trades  = func(df=df, or_levels=or_levels, **p)
    stats   = compute_metrics(trades)
    net_pnl = trades["pnl"].sum() if not trades.empty else 0

    tags = [
        f"OR={p['open_range_minutes']}m",
        "HoldToclose" if p["time_exit_minutes"] is None else f"TimeExit={p['time_exit_minutes']}m",
        f"MaxEntries={p['max_entries']}"
    ]
    if p["use_volume_filter"]: tags.append("VolFilter")
    if p["use_vwap_filter"]  : tags.append("VWAPFilter")
    if p["stop_loss"] is not None: tags.append(f"SL={p['stop_loss']:.3%}")
    if p["atr_stop_multiplier"] is not None: tags.append(f"ATRStop={p['atr_stop_multiplier']}x")
    if p["limit_same_direction"]: tags.append("OppositeAfterLoss")

    return {
        "strategy": func.__name__,
        "filters": " + ".join(tags),
        **p,
        "win_rate": stats["win_rate"],
        "profit_factor": stats["profit_factor"],
        "sharpe_ratio": stats["sharpe_ratio"],
        "max_drawdown": stats["max_drawdown"],
        "num_trades": stats["num_trades"],
        "net_pnl": round(net_pnl, 2),
        "daily_trades": trades.to_dict(orient="records")
    }


def _run_one_metrics_only(df, func, or_levels, **p):
    """Fast run – metrics only, no trade list"""
    trades  = func(df=df, or_levels=or_levels, **p)
    stats   = compute_metrics(trades)
    net_pnl = trades["pnl"].sum() if not trades.empty else 0
    return {
        "strategy": func.__name__,
        **p,
        "win_rate": stats["win_rate"],
        "profit_factor": stats["profit_factor"],
        "sharpe_ratio": stats["sharpe_ratio"],
        "max_drawdown": stats["max_drawdown"],
        "num_trades": stats["num_trades"],
        "net_pnl": round(net_pnl, 2)
    }


def _evaluate_metrics_only(df, or_levels, p):
    """Run every strategy once for a given param combo (metrics pass)."""
    res = []
    res.append(_run_one_metrics_only(df, backtest_orb,         or_levels, **p))
    res.append(_run_one_metrics_only(df, backtest_reverse_orb, or_levels, **p))
    res.append(_run_one_metrics_only(df, backtest_bbands,              None, **SINGLE_PASS_DEFAULTS))
    res.append(_run_one_metrics_only(df, backtest_support_resistance,  None, **SINGLE_PASS_DEFAULTS))
    return res


# ────────────────────────────────────────────────────────────────
# Main entry‑point
# ────────────────────────────────────────────────────────────────
def run_backtest_grid(ticker: str, days='30d', interval="5m", top_n=20):
    """
    Returns:
        {
            "scenarios": [ ... best‑N dicts incl. daily_trades ... ],
            "intraday_data": [ {timestamp, open, high, low, close, volume}, ... ]
        }
    """
    df_raw = fetch_intraday_data(ticker, days=days, interval=interval)
    if df_raw.empty:
        logger.warning("No data")
        return {"scenarios": [], "intraday_data": []}

    df        = _preprocess(df_raw)
    or_levels = _build_or_lookup(df, [5, 10, 15, 30, 45])

    workers = max(1, min(multiprocessing.cpu_count(), 8))
    scenarios_metrics = []

    # ───────── First pass – metrics only ─────────
    with parallel_backend("loky", inner_max_num_threads=1):
        scenarios_metrics = sum(
            Parallel(n_jobs=workers)(
                delayed(_evaluate_metrics_only)(df, or_levels, p) for p in param_grid()
            ),
            start=[]
        )

    # Sort & pick best
    scenarios_metrics.sort(key=lambda s: (s["win_rate"], s["profit_factor"] or 0), reverse=True)
    best_metrics = scenarios_metrics[:top_n]

    # ───────── Second pass – full trade logs ─────────
    best_complete = []
    for s in best_metrics:
        # extract the parameter dict expected by the strategy
        p = {k: s[k] for k in (
            "open_range_minutes", "use_volume_filter", "use_vwap_filter",
            "stop_loss", "atr_stop_multiplier", "time_exit_minutes",
            "limit_same_direction", "max_entries"
        )}
        # pick the right strategy
        strat_func = {
            "backtest_orb": backtest_orb,
            "backtest_reverse_orb": backtest_reverse_orb,
            "backtest_bbands": backtest_bbands,
            "backtest_support_resistance": backtest_support_resistance,
        }[s["strategy"]]

        best_complete.append(
            _run_one(df, strat_func, or_levels if "orb" in s["strategy"] else None, **p)
        )

    # Candles for the chart
    candles = (
        df_raw
        .reset_index(names="timestamp")
        .rename(columns={
            "open":"open", "high":"high", "low":"low",
            "close":"close", "volume":"volume"
        })
    )
    candles["timestamp"] = candles["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    return {"scenarios": best_complete, "intraday_data": candles.to_dict(orient="records")}
