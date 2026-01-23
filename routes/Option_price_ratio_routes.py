#option_price_ratio_routes.py
from flask import Blueprint, request, jsonify, Response, stream_with_context
from analysis.data_fetcher import fetch_stock_option_data, fetch_stock_fundamentals
from database.ticker_repository import get_all_tickers, get_logos_base64_for_symbols
from utils.auth import AuthError, authenticate_bearer_token
import datetime
import math
import os
import time
import json
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import pandas_market_calendars as mcal
try:
    import numpy as np
except Exception:  # pragma: no cover - optional dependency guard
    np = None
try:
    import pandas as pd
except Exception:  # pragma: no cover - optional dependency guard
    pd = None

option_price_ratio_blueprint = Blueprint('option_price_ratio', __name__)

def _get_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _safe_future_result(future, ticker: str, expiration: str):
    try:
        return future.result()
    except Exception as exc:
        return {
            "ticker": ticker,
            "expiration": expiration,
            "error": str(exc),
        }

def convert_nan(obj):
    """
    Recursively replace any NaN values in a structure with None.
    """
    if obj is None:
        return None
    if isinstance(obj, float) and math.isnan(obj):
        return None
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if np is not None:
        if isinstance(obj, np.generic):
            return convert_nan(obj.item())
        if isinstance(obj, np.ndarray):
            return [convert_nan(item) for item in obj.tolist()]
    if pd is not None:
        if obj is pd.NaT:
            return None
        if isinstance(obj, pd.Timestamp):
            if pd.isna(obj):
                return None
            return obj.isoformat()
    if isinstance(obj, dict):
        return {k: convert_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_nan(item) for item in obj]
    return obj


def _fetch_trailing_pe(ticker: str):
    try:
        fundamentals = fetch_stock_fundamentals(ticker, include_alpha=False)
        return fundamentals.get("trailingPE")
    except Exception:
        return None


def _compute_option_ratio(
    ticker: str,
    expiration: str,
    include_trailing_pe: bool = True,
    logo_map: dict | None = None,
):
    try:
        fetch_result = fetch_stock_option_data(
            ticker=ticker,
            expiration=expiration,
            all_expirations=False,
            option_type="puts",
        )

        stock_price = fetch_result.get("stock_price")
        option_data = fetch_result.get("option_data")

        logo_base64 = logo_map.get(ticker) if logo_map else None

        if stock_price is None:
            return {
                "ticker": ticker,
                "expiration": expiration,
                "logo_base64": logo_base64,
                "error": "Could not retrieve the latest trading price for the stock.",
            }

        if option_data is None or option_data.empty:
            return {
                "ticker": ticker,
                "expiration": expiration,
                "logo_base64": logo_base64,
                "error": "No puts data found for the given expiration.",
            }

        if "strike" not in option_data.columns or "lastPrice" not in option_data.columns:
            return {
                "ticker": ticker,
                "expiration": expiration,
                "logo_base64": logo_base64,
                "error": "Option chain payload missing required columns.",
            }

        strikes = option_data["strike"]
        otm_puts = option_data[strikes < stock_price]

        if otm_puts.empty:
            return {
                "ticker": ticker,
                "expiration": expiration,
                "logo_base64": logo_base64,
                "error": "No out-of-the-money puts found.",
            }

        best_idx = otm_puts["lastPrice"].idxmax()
        best_row = otm_puts.loc[best_idx].to_dict()
        best_price = best_row.get("lastPrice")
        best_ratio = best_price / stock_price if stock_price else None

        trailing_pe = None
        if include_trailing_pe:
            trailing_pe = _fetch_trailing_pe(ticker)

        return {
            "ticker": ticker,
            "expiration": expiration,
            "logo_base64": logo_base64,
            "stock_price": stock_price,
            "best_put_option": best_row,
            "best_put_price": best_price,
            "best_put_ratio": best_ratio,
            "trailingPE": trailing_pe,
        }
    except Exception as ticker_error:
        return {
            "ticker": ticker,
            "expiration": expiration,
            "logo_base64": logo_map.get(ticker) if logo_map else None,
            "error": str(ticker_error),
        }

def get_next_option_expiration(today=None):
    """
    Returns the option expiration date based on the following logic:
      - If today is Monday through Wednesday, use this week's Friday.
      - If today is Thursday through Sunday, use next week's Friday.
    If the computed Friday is a market holiday, this function adjusts it to the nearest previous trading day.
    
    Parameters:
      today (datetime.date): Optional; if not provided, defaults to today's date.
      
    Returns:
      datetime.date: The computed expiration date.
    """
    if today is None:
        today = datetime.date.today()
    
    # Determine target Friday based on day of the week.
    if today.weekday() <= 2:  # Monday (0) to Wednesday (2)
        days_until_friday = 4 - today.weekday()
    else:  # Thursday (3) through Sunday (6)
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
        try:
            auth = authenticate_bearer_token(request.headers.get("Authorization"))
        except AuthError as exc:
            return jsonify({"error": str(exc)}), 401

        # 1) Retrieve user-specific tickers from the database.
        tickers = get_all_tickers(user_id=auth.user_id)

        # 2) Compute the expiration date using the market calendar.
        expiration_date = get_next_option_expiration()
        expiration = expiration_date.strftime("%Y-%m-%d")

        if not tickers:
            return jsonify([]), 200

        try:
            max_workers = int(os.getenv("OPTION_PRICE_RATIO_MAX_WORKERS", "6"))
        except (TypeError, ValueError):
            max_workers = 6
        max_workers = max(1, min(max_workers, len(tickers)))
        timeout_seconds = _get_float_env("OPTION_PRICE_RATIO_TIMEOUT_SECONDS", 120.0)
        deadline = None
        if timeout_seconds > 0:
            deadline = time.monotonic() + timeout_seconds

        logo_map = get_logos_base64_for_symbols(tickers)

        def _process_ticker(ticker: str):
            return _compute_option_ratio(
                ticker,
                expiration,
                include_trailing_pe=True,
                logo_map=logo_map,
            )

        # 3) Fetch in parallel (network-bound).
        results_by_ticker = {}
        pending_tickers = []
        if max_workers == 1:
            for ticker in tickers:
                if deadline is not None and time.monotonic() >= deadline:
                    pending_tickers = [t for t in tickers if t not in results_by_ticker]
                    break
                results_by_ticker[ticker] = _process_ticker(ticker)
        else:
            executor = ThreadPoolExecutor(max_workers=max_workers)
            futures = {executor.submit(_process_ticker, t): t for t in tickers}
            pending = set(futures)
            try:
                while pending:
                    remaining = None
                    if deadline is not None:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            break
                    done, pending = wait(pending, timeout=remaining, return_when=FIRST_COMPLETED)
                    if not done:
                        break
                    for fut in done:
                        ticker = futures[fut]
                        results_by_ticker[ticker] = _safe_future_result(fut, ticker, expiration)
            finally:
                if pending:
                    pending_tickers = [futures[fut] for fut in pending]
                    executor.shutdown(wait=False, cancel_futures=True)
                else:
                    executor.shutdown(wait=True)

        if pending_tickers:
            timeout_error = "Timed out waiting for option data."
            for ticker in pending_tickers:
                results_by_ticker[ticker] = {
                    "ticker": ticker,
                    "expiration": expiration,
                    "error": timeout_error,
                }

        results = [results_by_ticker[t] for t in tickers if t in results_by_ticker]

        ok_items = [item for item in results if not item.get("error")]
        error_items = [item for item in results if item.get("error")]
        ok_items.sort(
            key=lambda item: item.get("best_put_ratio") if item.get("best_put_ratio") is not None else -1,
            reverse=True,
        )
        results = ok_items + error_items

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
    try:
        auth = authenticate_bearer_token(request.headers.get("Authorization"))
    except AuthError as exc:
        return jsonify({"error": str(exc)}), 401

    tickers = get_all_tickers(user_id=auth.user_id)
    expiration_date = get_next_option_expiration()
    expiration = expiration_date.strftime("%Y-%m-%d")
    logo_map = get_logos_base64_for_symbols(tickers)

    try:
        max_workers = int(os.getenv("OPTION_PRICE_RATIO_MAX_WORKERS", "6"))
    except (TypeError, ValueError):
        max_workers = 6
    max_workers = max(1, min(max_workers, max(1, len(tickers))))
    try:
        pe_workers = int(os.getenv("OPTION_PRICE_RATIO_PE_MAX_WORKERS", "3"))
    except (TypeError, ValueError):
        pe_workers = 3
    pe_workers = max(1, min(pe_workers, max(1, len(tickers))))
    stream_timeout_seconds = _get_float_env("OPTION_PRICE_RATIO_STREAM_TIMEOUT_SECONDS", 120.0)
    deadline = None
    if stream_timeout_seconds > 0:
        deadline = time.monotonic() + stream_timeout_seconds

    def _sse_event(event: str, payload) -> str:
        data = json.dumps(convert_nan(payload))
        return f"event: {event}\ndata: {data}\n\n"

    @stream_with_context
    def generate():
        if not tickers:
            yield _sse_event("done", {"expiration": expiration})
            return

        ratio_done_sent = False
        if max_workers == 1 and pe_workers == 1:
            completed = set()
            items_by_ticker = {}
            for ticker in tickers:
                if deadline is not None and time.monotonic() >= deadline:
                    break
                completed.add(ticker)
                item = _compute_option_ratio(
                    ticker,
                    expiration,
                    include_trailing_pe=False,
                    logo_map=logo_map,
                )
                items_by_ticker[ticker] = item
                yield _sse_event("item", item)
            if deadline is not None and time.monotonic() >= deadline:
                timeout_error = "Timed out waiting for option data."
                for ticker in tickers:
                    if ticker in completed:
                        continue
                    yield _sse_event(
                        "item",
                        {"ticker": ticker, "expiration": expiration, "error": timeout_error},
                    )
                yield _sse_event("ratio_done", {"expiration": expiration})
                yield _sse_event("done", {"expiration": expiration})
                return
            yield _sse_event("ratio_done", {"expiration": expiration})
            for ticker, item in items_by_ticker.items():
                if item.get("error"):
                    continue
                pe_value = _fetch_trailing_pe(ticker)
                yield _sse_event("pe", {"ticker": ticker, "trailingPE": pe_value})
            yield _sse_event("done", {"expiration": expiration})
            return

        ratio_executor = ThreadPoolExecutor(max_workers=max_workers)
        pe_executor = ThreadPoolExecutor(max_workers=pe_workers)
        ratio_futures = {
            ratio_executor.submit(_compute_option_ratio, t, expiration, False, logo_map): t
            for t in tickers
        }
        pe_futures = {}
        pending_ratio = set(ratio_futures)
        pending_pe = set()
        try:
            while pending_ratio or pending_pe:
                remaining = None
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                wait_on = pending_ratio | pending_pe
                if not wait_on:
                    break
                done, pending = wait(wait_on, timeout=remaining, return_when=FIRST_COMPLETED)
                if not done:
                    break
                pending_ratio = {f for f in pending_ratio if f in pending}
                pending_pe = {f for f in pending_pe if f in pending}
                for fut in done:
                    if fut in ratio_futures:
                        ticker = ratio_futures[fut]
                        item = _safe_future_result(fut, ticker, expiration)
                        if "trailingPE" not in item:
                            item["trailingPE"] = None
                        yield _sse_event("item", item)
                        if not item.get("error"):
                            pe_fut = pe_executor.submit(_fetch_trailing_pe, ticker)
                            pe_futures[pe_fut] = ticker
                            pending_pe.add(pe_fut)
                    elif fut in pe_futures:
                        ticker = pe_futures[fut]
                        pe_value = None
                        try:
                            pe_value = fut.result()
                        except Exception as exc:
                            yield _sse_event(
                                "pe",
                                {"ticker": ticker, "trailingPE": None, "error": str(exc)},
                            )
                        else:
                            yield _sse_event("pe", {"ticker": ticker, "trailingPE": pe_value})
                if not pending_ratio and not ratio_done_sent:
                    ratio_done_sent = True
                    yield _sse_event("ratio_done", {"expiration": expiration})
            if pending_ratio:
                timeout_error = "Timed out waiting for option data."
                for fut in pending_ratio:
                    ticker = ratio_futures[fut]
                    yield _sse_event(
                        "item",
                        {"ticker": ticker, "expiration": expiration, "error": timeout_error},
                    )
                if not ratio_done_sent:
                    ratio_done_sent = True
                    yield _sse_event("ratio_done", {"expiration": expiration})
            if pending_pe:
                timeout_error = "Timed out waiting for trailing PE."
                for fut in pending_pe:
                    ticker = pe_futures[fut]
                    yield _sse_event(
                        "pe",
                        {"ticker": ticker, "trailingPE": None, "error": timeout_error},
                    )
        finally:
            if pending_ratio:
                ratio_executor.shutdown(wait=False, cancel_futures=True)
            else:
                ratio_executor.shutdown(wait=True)
            if pending_pe:
                pe_executor.shutdown(wait=False, cancel_futures=True)
            else:
                pe_executor.shutdown(wait=True)
        if not ratio_done_sent:
            yield _sse_event("ratio_done", {"expiration": expiration})
        yield _sse_event("done", {"expiration": expiration})

    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
        "Connection": "keep-alive",
    }
    return Response(generate(), mimetype="text/event-stream", headers=headers)
