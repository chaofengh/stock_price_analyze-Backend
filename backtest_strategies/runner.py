"""
Grid‑search runner – now:
  • builds ATR + VWAP *once*
  • pre‑computes OR‑levels for every open‑range length
  • parallelises the 10 240 parameter pairs across CPU cores
"""
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing, datetime, pandas as pd
from .data_fetcher import fetch_intraday_data
from .param_grid import generate as param_grid
from .metrics import compute_metrics
from .logging_config import logger
from .orb_strategies import (
    compute_atr,
    compute_intraday_vwap,
    backtest_orb,
    backtest_reverse_orb,
)
from .date_utils import is_us_eastern_dst

# ---------- helpers ----------
def _preprocess(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["atr"]   = compute_atr(df)
    df["vwap"]  = compute_intraday_vwap(df)
    df["date_utc"] = df.index.strftime("%Y-%m-%d")
    return df.dropna(subset=["atr", "vwap"])

def _build_or_lookup(df: pd.DataFrame, or_minutes_list) -> dict:
    """
    or_levels[m][day] = (or_high, or_low)
    """
    lookup = {m: {} for m in or_minutes_list}
    for day_str, day_df in df.groupby("date_utc"):
        date_val = datetime.datetime.strptime(day_str, "%Y-%m-%d").date()
        # dst‑aware session
        start, end = ("13:30", "20:00") if is_us_eastern_dst(date_val) else ("14:30", "21:00")
        session = day_df.between_time(start, end)
        if session.empty:
            continue
        bar_min = int((session.index[1] - session.index[0]).seconds / 60)
        for m in or_minutes_list:
            or_bars = max(1, m // bar_min)
            block   = session.iloc[:or_bars]
            lookup[m][day_str] = (block["High"].max(), block["Low"].min())
    return lookup

# ---------- _run_one kept identical except for new arg ----------
def _run_one(df, func, or_levels, **p):
    trades  = func(df=df, or_levels=or_levels, **p)
    stats   = compute_metrics(trades)
    net_pnl = trades["pnl"].sum() if not trades.empty else 0

    tags = [
        f"OR={p['open_range_minutes']}m",
        "HoldToClose" if p["time_exit_minutes"] is None else f"TimeExit={p['time_exit_minutes']}m",
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

# ---------- parallel executor ----------
_global_df, _global_or = None, None  # set once in every worker

def _init_pool(df, or_levels):
    global _global_df, _global_or
    _global_df, _global_or = df, or_levels

def _evaluate(p):
    res1 = _run_one(_global_df, backtest_orb,            _global_or, **p)
    res2 = _run_one(_global_df, backtest_reverse_orb,    _global_or, **p)
    return [res1, res2]

def run_backtest_grid(ticker: str, days=30, interval="5m", top_n=10):
    df_raw = fetch_intraday_data(ticker, days=days, interval=interval)
    if df_raw.empty:
        logger.warning("No data")
        return {"scenarios": [], "intraday_data": []}

    df        = _preprocess(df_raw)
    or_levels = _build_or_lookup(df, [5, 10, 15, 30, 45])  # keep in sync with param_grid

    # pool size = physical cores unless the machine is tiny
    workers = max(1, min( multiprocessing.cpu_count(), 8))
    scenarios = []

    with ProcessPoolExecutor(
        max_workers=workers,
        initializer=_init_pool,
        initargs=(df, or_levels),
    ) as pool:
        futures = [pool.submit(_evaluate, p) for p in param_grid()]
        for fut in as_completed(futures):
            scenarios.extend(fut.result())

    scenarios.sort(key=lambda s: (s["win_rate"], s["profit_factor"] or 0), reverse=True)
    best = scenarios[:top_n]

    candles = (df_raw.reset_index(names="timestamp")
                  .rename(columns={"Open":"open","High":"high","Low":"low","Close":"close","Volume":"volume"}))
    candles["timestamp"] = candles["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    return {"scenarios": best, "intraday_data": candles.to_dict(orient="records")}
