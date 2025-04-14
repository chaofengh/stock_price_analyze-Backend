# backtest_strategies/metrics.py

import numpy as np
import pandas as pd

def compute_metrics(trades):
    if trades.empty or "pnl" not in trades.columns:
        return {
            "win_rate": 0.0,
            "profit_factor": None,
            "sharpe_ratio": None,
            "max_drawdown": None,
            "num_trades": 0
        }
    pnl_series = pd.Series(trades['pnl'])
    wins = pnl_series[pnl_series > 0]
    losses = pnl_series[pnl_series <= 0]
    win_rate = len(wins) / len(pnl_series) if len(pnl_series) > 0 else 0.0
    total_win = wins.sum()
    total_loss = abs(losses.sum())
    profit_factor = total_win / total_loss if total_loss != 0 else np.nan

    mean_return = pnl_series.mean()
    std_return = pnl_series.std()
    risk_free_rate = 0.0
    sharpe_ratio = (mean_return - risk_free_rate) / std_return if std_return != 0 else np.nan

    equity_curve = pnl_series.cumsum()
    rolling_max = equity_curve.cummax()
    max_drawdown = (equity_curve - rolling_max).min()

    return {
        "win_rate": round(win_rate, 3),
        "profit_factor": round(profit_factor, 3) if profit_factor == profit_factor else None,
        "sharpe_ratio": round(sharpe_ratio, 3) if sharpe_ratio == sharpe_ratio else None,
        "max_drawdown": round(max_drawdown, 2) if max_drawdown == max_drawdown else None,
        "num_trades": len(trades)
    }
