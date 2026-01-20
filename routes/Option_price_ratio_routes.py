#option_price_ratio_routes.py
from flask import Blueprint, request, jsonify, Response, stream_with_context
from analysis.data_fetcher import fetch_stock_option_data, fetch_stock_fundamentals
from database.ticker_repository import get_all_tickers
import datetime
import math
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas_market_calendars as mcal

option_price_ratio_blueprint = Blueprint('option_price_ratio', __name__)

def convert_nan(obj):
    """
    Recursively replace any NaN values in a structure with None.
    """
    if isinstance(obj, float) and math.isnan(obj):
        return None
    elif isinstance(obj, dict):
        return {k: convert_nan(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_nan(item) for item in obj]
    else:
        return obj

def get_next_option_expiration(today=None):
    """
    Returns the next option expiration date based on the following logic:
      - If today is Monday through Thursday, use the upcoming Friday.
      - If today is Friday, Saturday, or Sunday, use next week's Friday.
    If the computed Friday is a market holiday, this function adjusts it to the nearest previous trading day.
    
    Parameters:
      today (datetime.date): Optional; if not provided, defaults to today's date.
      
    Returns:
      datetime.date: The computed expiration date.
    """
    if today is None:
        today = datetime.date.today()
    
    # Determine upcoming Friday based on day of the week
    if today.weekday() < 4:  # Monday (0) to Thursday (3)
        days_until_friday = 4 - today.weekday()
    else:  # Friday (4), Saturday (5), or Sunday (6)
        days_until_friday = 11 - today.weekday()
    
    expiration_date = today + datetime.timedelta(days=days_until_friday)
    
    # Obtain the NYSE schedule for a window around the computed expiration date.
    nyse = mcal.get_calendar('NYSE')
    start_date = (expiration_date - datetime.timedelta(days=3)).strftime("%Y-%m-%d")
    end_date   = (expiration_date + datetime.timedelta(days=3)).strftime("%Y-%m-%d")
    schedule = nyse.schedule(start_date=start_date, end_date=end_date)
    
    # Extract open trading days from the schedule
    open_days = set([d.date() for d in schedule.index])
    
    # If the computed expiration date is not a trading day, adjust to the nearest previous trading day.
    while expiration_date not in open_days and expiration_date >= today:
        expiration_date -= datetime.timedelta(days=1)
    
    return expiration_date

@option_price_ratio_blueprint.route('/api/option-price-ratio', methods=['GET'])
def get_option_price_ratio():
    """
    GET endpoint to fetch the out-of-the-money put option with the highest price
    for all tickers in the database, and include the trailing PE for each stock.
    
    Expiration date is automatically set to:
      - The upcoming Friday if today is Monday-Thursday.
      - Next week's Friday if today is Friday, Saturday, or Sunday.
    If the computed Friday is a market holiday, the expiration date will be adjusted
    to the previous trading day.
    
    Returns:
      A JSON list of objects, each representing one ticker. Each object has:
        - ticker
        - expiration
        - stock_price
        - best_put_option (details about the best out-of-the-money put)
        - best_put_price
        - best_put_ratio (best_put_price / stock_price)
        - trailingPE (from the stock fundamentals)
        - error (only if something failed for that ticker)
    """
    try:
        # 1) Retrieve all tickers from the database.
        tickers = get_all_tickers()

        # 2) Compute the expiration date using the market calendar.
        expiration_date = get_next_option_expiration()
        expiration = expiration_date.strftime("%Y-%m-%d")

        if not tickers:
            return jsonify([]), 200

        try:
            max_workers = int(os.getenv("OPTION_PRICE_RATIO_MAX_WORKERS", "12"))
        except (TypeError, ValueError):
            max_workers = 12
        max_workers = max(1, min(max_workers, len(tickers)))

        def _process_ticker(ticker: str):
            try:
                fetch_result = fetch_stock_option_data(
                    ticker=ticker,
                    expiration=expiration,
                    all_expirations=False,
                    option_type="puts"
                )

                stock_price = fetch_result.get("stock_price")
                option_data = fetch_result.get("option_data")

                if stock_price is None:
                    return {
                        "ticker": ticker,
                        "expiration": expiration,
                        "error": "Could not retrieve the latest trading price for the stock."
                    }

                if option_data is None or option_data.empty:
                    return {
                        "ticker": ticker,
                        "expiration": expiration,
                        "error": "No puts data found for the given expiration."
                    }

                if "strike" not in option_data.columns or "lastPrice" not in option_data.columns:
                    return {
                        "ticker": ticker,
                        "expiration": expiration,
                        "error": "Option chain payload missing required columns."
                    }

                # Filter for out-of-the-money put options (strike < stock_price).
                strikes = option_data["strike"]
                otm_puts = option_data[strikes < stock_price]

                if otm_puts.empty:
                    return {
                        "ticker": ticker,
                        "expiration": expiration,
                        "error": "No out-of-the-money puts found."
                    }

                # Select the put option with the highest 'lastPrice'.
                best_idx = otm_puts['lastPrice'].idxmax()
                best_row = otm_puts.loc[best_idx].to_dict()
                best_price = best_row.get('lastPrice')
                best_ratio = best_price / stock_price if stock_price else None

                return {
                    "ticker": ticker,
                    "expiration": expiration,
                    "stock_price": stock_price,
                    "best_put_option": best_row,
                    "best_put_price": best_price,
                    "best_put_ratio": best_ratio,
                    "trailingPE": None
                }

            except Exception as ticker_error:
                return {
                    "ticker": ticker,
                    "expiration": expiration,
                    "error": str(ticker_error)
                }

        # 3) Fetch in parallel (network-bound).
        if max_workers == 1:
            results = [_process_ticker(t) for t in tickers]
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                results = list(executor.map(_process_ticker, tickers))

        # Convert any NaN values in the results to None.
        safe_results = convert_nan(results)
        return jsonify(safe_results), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@option_price_ratio_blueprint.route('/api/option-price-ratio/stream', methods=['GET'])
def stream_option_price_ratio():
    """
    Streams per-ticker option ratio results as Server-Sent Events (SSE).
    Each message is a JSON object with the same shape as items from /api/option-price-ratio.
    """
    tickers = get_all_tickers()
    expiration_date = get_next_option_expiration()
    expiration = expiration_date.strftime("%Y-%m-%d")

    try:
        max_workers = int(os.getenv("OPTION_PRICE_RATIO_MAX_WORKERS", "12"))
    except (TypeError, ValueError):
        max_workers = 12
    max_workers = max(1, min(max_workers, max(1, len(tickers))))

    def _compute_one(ticker: str):
        try:
            fetch_result = fetch_stock_option_data(
                ticker=ticker,
                expiration=expiration,
                all_expirations=False,
                option_type="puts",
            )
            stock_price = fetch_result.get("stock_price")
            option_data = fetch_result.get("option_data")

            if stock_price is None:
                return {
                    "ticker": ticker,
                    "expiration": expiration,
                    "error": "Could not retrieve the latest trading price for the stock.",
                }

            if option_data is None or option_data.empty:
                return {
                    "ticker": ticker,
                    "expiration": expiration,
                    "error": "No puts data found for the given expiration.",
                }

            if "strike" not in option_data.columns or "lastPrice" not in option_data.columns:
                return {
                    "ticker": ticker,
                    "expiration": expiration,
                    "error": "Option chain payload missing required columns.",
                }

            strikes = option_data["strike"]
            otm_puts = option_data[strikes < stock_price]
            if otm_puts.empty:
                return {
                    "ticker": ticker,
                    "expiration": expiration,
                    "error": "No out-of-the-money puts found.",
                }

            best_idx = otm_puts["lastPrice"].idxmax()
            best_row = otm_puts.loc[best_idx].to_dict()
            best_price = best_row.get("lastPrice")
            best_ratio = best_price / stock_price if stock_price else None

            return {
                "ticker": ticker,
                "expiration": expiration,
                "stock_price": stock_price,
                "best_put_option": best_row,
                "best_put_price": best_price,
                "best_put_ratio": best_ratio,
                "trailingPE": None,
            }
        except Exception as ticker_error:
            return {"ticker": ticker, "expiration": expiration, "error": str(ticker_error)}

    def _sse_event(event: str, payload) -> str:
        data = json.dumps(convert_nan(payload))
        return f"event: {event}\ndata: {data}\n\n"

    @stream_with_context
    def generate():
        if not tickers:
            yield _sse_event("done", {"expiration": expiration})
            return

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_compute_one, t): t for t in tickers}
            for fut in as_completed(futures):
                yield _sse_event("item", fut.result())
        yield _sse_event("done", {"expiration": expiration})

    return Response(generate(), mimetype="text/event-stream")


@option_price_ratio_blueprint.route('/api/option-price-ratio/trailing-pe', methods=['GET'])
def get_option_price_ratio_trailing_pe():
    """
    Batch endpoint to fetch trailing PE for a small set of tickers.
    Example: GET /api/option-price-ratio/trailing-pe?tickers=AAPL,MSFT
    """
    raw = request.args.get("tickers", "") or ""
    tickers = [t.strip().upper() for t in raw.split(",") if t.strip()]
    tickers = list(dict.fromkeys(tickers))

    if not tickers:
        return jsonify({}), 200

    try:
        max_workers = int(os.getenv("OPTION_PRICE_RATIO_PE_MAX_WORKERS", "8"))
    except (TypeError, ValueError):
        max_workers = 8
    max_workers = max(1, min(max_workers, len(tickers)))

    def _fetch_pe(ticker: str):
        try:
            fundamentals = fetch_stock_fundamentals(ticker, include_alpha=False)
            return ticker, fundamentals.get("trailingPE")
        except Exception:
            return ticker, None

    if max_workers == 1:
        pairs = [_fetch_pe(t) for t in tickers]
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            pairs = list(executor.map(_fetch_pe, tickers))

    return jsonify({k: v for (k, v) in pairs}), 200
