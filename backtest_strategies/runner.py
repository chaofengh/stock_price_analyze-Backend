# backtest_strategies/runner.py

import pandas as pd
from .data_fetcher import fetch_intraday_data
from .orb_strategies import backtest_orb, backtest_reverse_orb
from .metrics import compute_metrics
from .logging_config import logger

def run_backtest(ticker, days=30, interval="5m", strategy="opening_range_breakout"):
    """
    High-level backtest function:
      1) Fetch intraday data.
      2) Depending on 'strategy', run the appropriate scenario(s).
      3) Return scenario results plus intraday data for charting.
    """
    df = fetch_intraday_data(ticker, days=days, interval=interval)
    if df.empty:
        logger.warning(f"No intraday data available for {ticker}.")
        return {"scenarios": [], "intraday_data": []}

    # Example parameter sweeps for demonstration
    or_minutes_list = [30, 45]
    volume_options = [False, True]
    stop_loss_options = [None, 0.005, 0.01]
    time_exit_options = [60, 120]
    limit_same_direction_options = [False, True]
    max_entry_options = [1, 2]

    results = []

    # Select the appropriate backtest function
    if strategy == "opening_range_breakout":
        backtest_func = backtest_orb
    elif strategy == "reverse_opening_range_breakout":
        backtest_func = backtest_reverse_orb
    else:
        raise ValueError(f"Unknown strategy: {strategy}")

    for or_minutes in or_minutes_list:
        for use_vol in volume_options:
            for sl in stop_loss_options:
                for te in time_exit_options:
                    for limit_dir in limit_same_direction_options:
                        for max_entries in max_entry_options:
                            trades = backtest_func(
                                df=df,
                                open_range_minutes=or_minutes,
                                use_volume_filter=use_vol,
                                stop_loss=sl,
                                time_exit_minutes=te,
                                limit_same_direction=limit_dir,
                                max_entries=max_entries
                            )
                            metrics = compute_metrics(trades)
                            daily_trades = trades.to_dict(orient="records")
                            net_pnl = trades["pnl"].sum() if not trades.empty else 0

                            filters_list = []
                            filters_list.append(f"OR={or_minutes}min")
                            if use_vol:
                                filters_list.append("VolumeFilter")
                            if sl is not None:
                                filters_list.append(f"StopLoss={sl}")
                            filters_list.append(f"TimeExit={te}m")
                            if limit_dir:
                                filters_list.append("ForceOppositeAfterLoss")
                            filters_list.append(f"MaxEntries={max_entries}")

                            scenario_result = {
                                "scenario_name": strategy,
                                "filters": " + ".join(filters_list),
                                "open_range_minutes": or_minutes,
                                "use_volume_filter": use_vol,
                                "stop_loss": sl,
                                "time_exit_minutes": te,
                                "limit_same_direction": limit_dir,
                                "max_entries": max_entries,
                                "win_rate": metrics["win_rate"],
                                "win_rate_formatted": f"{round(metrics['win_rate']*100,1)}%",
                                "profit_factor": metrics["profit_factor"],
                                "profit_factor_formatted": (
                                    f"{metrics['profit_factor']:.2f}"
                                    if metrics["profit_factor"] is not None else "N/A"
                                ),
                                "sharpe_ratio": metrics["sharpe_ratio"],
                                "sharpe_ratio_formatted": (
                                    f"{metrics['sharpe_ratio']:.2f}"
                                    if metrics["sharpe_ratio"] is not None else "N/A"
                                ),
                                "max_drawdown": metrics["max_drawdown"],
                                "max_drawdown_formatted": (
                                    f"{metrics['max_drawdown']:.2f}"
                                    if metrics["max_drawdown"] is not None else "N/A"
                                ),
                                "num_trades": metrics["num_trades"],
                                "daily_trades": daily_trades,
                                "net_pnl": round(net_pnl, 2),
                                "net_pnl_formatted": f"${round(net_pnl, 2):,}"
                            }
                            results.append(scenario_result)

    # Prepare intraday data for charting
    intraday_df = df.reset_index(names="timestamp").rename(columns={"index": "timestamp"})
    intraday_df["date"] = intraday_df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    intraday_df.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume"
        },
        inplace=True
    )
    intraday_data = intraday_df.to_dict(orient="records")

    return {
        "scenarios": results,
        "intraday_data": intraday_data
    }

# Optional: allow command-line execution
if __name__ == "__main__":
    # Example: run the backtest for ticker "AAPL"
    results = run_backtest("AAPL", days=30, interval="5m", strategy="opening_range_breakout")
    print(results)
