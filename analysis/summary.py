# summary.py

import pandas as pd
from .data_preparation import prepare_stock_data, get_trading_period
from .event_detection_analysis import get_touch_and_hug_events, compute_bounces_and_pullbacks
from .metrics_calculation import compute_aggregates
from .additional_metrics import compute_additional_metrics, compute_avg_hug_length
from .chart_builder import build_chart_data
from .fundamentals import (
    get_fundamentals, 
    get_peers, 
    get_peers_fundamentals, 
    compute_peer_metric_avg, 
    compare_metric
)
from .data_fetcher import fetch_stock_data  # <--- Make sure we import fetch_stock_data

def get_summary(symbol: str) -> dict:
    # 1. Fetch and prepare data
    data_dict = prepare_stock_data(symbol)
    if symbol not in data_dict:
        raise ValueError(f"No data found for symbol {symbol}")
    df = data_dict[symbol]
    analysis_period, initial_price, final_price = get_trading_period(df)
    n = len(df)
    
    # 2. Detect touches & hug events
    touches, hug_events_upper, hug_events_lower = get_touch_and_hug_events(df)
    
    # 3. Compute bounce/pullback events
    results_window_5 = compute_bounces_and_pullbacks(df, touches, hug_events_upper, hug_events_lower, window=5)
    results_window_10 = compute_bounces_and_pullbacks(df, touches, hug_events_upper, hug_events_lower, window=10)
    
    # 4. Compute aggregated metrics
    aggregated_window_5 = compute_aggregates(results_window_5, hug_events_upper, hug_events_lower)
    aggregated_window_10 = compute_aggregates(results_window_10, hug_events_upper, hug_events_lower)
    
    # 5. Compute additional metrics and average hug lengths
    add_metrics = compute_additional_metrics(df, initial_price, final_price)
    avg_upper_hug_length = compute_avg_hug_length(hug_events_upper)
    avg_lower_hug_length = compute_avg_hug_length(hug_events_lower)
    
    # 6. Build chart data
    chart_data = build_chart_data(df, touches, hug_events_upper + hug_events_lower)
    
    # 7. Fundamentals and peer analysis
    fundamentals = get_fundamentals(symbol)
    peers = get_peers(symbol)
    print('this is values of peers:', peers)
    peers_fundamentals = get_peers_fundamentals(peers)
    avg_peer_trailingPE = compute_peer_metric_avg(peers_fundamentals, "trailingPE")
    avg_peer_forwardPE = compute_peer_metric_avg(peers_fundamentals, "forwardPE")
    avg_peer_PEG = compute_peer_metric_avg(peers_fundamentals, "PEG")
    avg_peer_PGI = compute_peer_metric_avg(peers_fundamentals, "PGI")
    avg_peer_beta = compute_peer_metric_avg(peers_fundamentals, "beta")
    
    trailingPE_compare = compare_metric(fundamentals.get("trailingPE"), avg_peer_trailingPE, "Trailing PE")
    forwardPE_compare  = compare_metric(fundamentals.get("forwardPE"),  avg_peer_forwardPE,  "Forward PE")
    PEG_compare        = compare_metric(fundamentals.get("PEG"),        avg_peer_PEG,        "PEG")
    PGI_compare        = compare_metric(fundamentals.get("PGI"),        avg_peer_PGI,        "PGI")
    beta_compare       = compare_metric(fundamentals.get("beta"),       avg_peer_beta,       "Beta")
    
    # 7a. Fetch the latest peer data for 1 day, 15-minute intervals
    #     and calculate each peer's last close and percentage change for the day.
    peer_info = {}
    if peers:
        peer_data = fetch_stock_data(peers, period="1d", interval="15m")
        for peer_symbol in peers:
            df_peer = peer_data.get(peer_symbol, pd.DataFrame())
            if not df_peer.empty:
                # Get the first and last close of the day
                first_close = df_peer.iloc[0]['close']
                last_close = df_peer.iloc[-1]['close']
                
                # Calculate the percentage change if we have valid data
                if first_close is not None and first_close != 0:
                    pct_change = ((last_close - first_close) / first_close) * 100
                else:
                    pct_change = None
                
                peer_info[peer_symbol] = {
                    'latest_price': last_close,
                    'percentage_change': pct_change
                }
            else:
                # No data for this peer
                peer_info[peer_symbol] = {
                    'latest_price': None,
                    'percentage_change': None
                }

    # 8. Fetch income statement data
    try:
        from .data_fetcher import fetch_income_statement  # import here if not used elsewhere
        income_statement = fetch_income_statement(symbol)
    except Exception:
        income_statement = {}
    
    # 9. Compile the summary dictionary
    summary = {
        'symbol': symbol,
        'analysis_period': analysis_period,
        'trading_days': n,
        'initial_price': initial_price,
        'final_price': final_price,
        'price_change_in_dollars': final_price - initial_price,
        'percentage_change': add_metrics['percentage_change'],
        'total_volume': add_metrics['total_volume'],
        'average_volume': add_metrics['average_volume'],
        'max_price': add_metrics['max_price'],
        'min_price': add_metrics['min_price'],
        'volatility': add_metrics['volatility'],
        'average_BB_width': add_metrics['avg_BB_width'],
        'RSI_mean': add_metrics['RSI_mean'],
        'RSI_std': add_metrics['RSI_std'],

        'total_touches': len(touches),
        'upper_touches_count': len([t for t in touches if t['band'] == 'upper']),
        'lower_touches_count': len([t for t in touches if t['band'] == 'lower']),

        'hug_events_upper_count': len(hug_events_upper),
        'hug_events_lower_count': len(hug_events_lower),
        'avg_upper_hug_length': avg_upper_hug_length,
        'avg_lower_hug_length': avg_lower_hug_length,

        'chart_data': chart_data,

        'window_5': results_window_5,
        'window_10': results_window_10,

        'aggregated_window_5': aggregated_window_5,
        'aggregated_window_10': aggregated_window_10,

        'trailingPE': fundamentals.get("trailingPE"),
        'forwardPE': fundamentals.get("forwardPE"),
        'PEG': fundamentals.get("PEG"),
        'PGI': fundamentals.get("PGI"),
        'trailingPEG': fundamentals.get("trailingPEG"),
        'dividendYield': fundamentals.get("dividendYield"),
        'beta': fundamentals.get("beta"),
        'marketCap': fundamentals.get("marketCap"),
        'priceToBook': fundamentals.get("priceToBook"),
        'forwardEPS': fundamentals.get("forwardEPS"),
        'trailingEPS': fundamentals.get("trailingEPS"),
        'debtToEquity': fundamentals.get("debtToEquity"),

        'peers': peers,
        'peers_fundamentals': peers_fundamentals,
        'avg_peer_trailingPE': avg_peer_trailingPE,
        'avg_peer_forwardPE': avg_peer_forwardPE,
        'avg_peer_PEG': avg_peer_PEG,
        'avg_peer_PGI': avg_peer_PGI,
        'avg_peer_beta': avg_peer_beta,

        'peer_comparisons': {
            'trailingPE': trailingPE_compare,
            'forwardPE': forwardPE_compare,
            'PEG': PEG_compare,
            'PGI': PGI_compare,
            'beta': beta_compare,
        },

        'income_statement': income_statement,

        # Include the new peer info with latest price and daily percentage change:
        'peer_latest_data': peer_info
    }

    return summary
