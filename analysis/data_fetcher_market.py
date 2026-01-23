"""
data_fetcher_market.py
Purpose: fetch OHLCV data and option chains from yfinance.
"""
import os
import threading
import time

import numpy as np
import pandas as pd
import yfinance as yf

from .financials_yfinance import build_income_annual_from_yfinance


def _price_from_underlying(underlying):
    if not isinstance(underlying, dict):
        return None
    for key in (
        "regularMarketPrice",
        "lastPrice",
        "mark",
        "bid",
        "ask",
        "previousClose",
    ):
        value = underlying.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


_YF_RATE_LOCK = threading.Lock()
_YF_NEXT_ALLOWED_TS = 0.0


def _get_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _get_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _is_rate_limit_error(exc: Exception) -> bool:
    name = exc.__class__.__name__
    if name == "YFRateLimitError":
        return True
    message = str(exc).lower()
    return "rate limit" in message or "too many requests" in message


def _apply_rate_limit_cooldown():
    cooldown = max(0.0, _get_float_env("YF_RATE_LIMIT_COOLDOWN_SECONDS", 5.0))
    if cooldown <= 0:
        return
    global _YF_NEXT_ALLOWED_TS
    with _YF_RATE_LOCK:
        now = time.monotonic()
        _YF_NEXT_ALLOWED_TS = max(_YF_NEXT_ALLOWED_TS, now + cooldown)


def _throttle_yfinance():
    min_interval = max(0.0, _get_float_env("YF_RATE_LIMIT_SECONDS", 0.75))
    if min_interval <= 0:
        return
    global _YF_NEXT_ALLOWED_TS
    with _YF_RATE_LOCK:
        now = time.monotonic()
        wait = _YF_NEXT_ALLOWED_TS - now
        if wait > 0:
            time.sleep(wait)
            now = time.monotonic()
        _YF_NEXT_ALLOWED_TS = now + min_interval


def _option_chain_with_retry(ticker_obj, expiration: str):
    retries = max(0, _get_int_env("YF_OPTION_RETRIES", 2))
    backoff = max(0.0, _get_float_env("YF_OPTION_RETRY_BACKOFF_SECONDS", 1.0))
    last_exc = None
    for attempt in range(retries + 1):
        _throttle_yfinance()
        try:
            chain = ticker_obj.option_chain(expiration)
            if chain is not None:
                return chain
        except Exception as exc:  # pragma: no cover - exercised in integration
            last_exc = exc
            if _is_rate_limit_error(exc):
                _apply_rate_limit_cooldown()
        if attempt < retries:
            sleep_seconds = backoff * (2**attempt)
            if last_exc is not None and _is_rate_limit_error(last_exc):
                cooldown = max(0.0, _get_float_env("YF_RATE_LIMIT_COOLDOWN_SECONDS", 5.0))
                sleep_seconds = max(sleep_seconds, cooldown)
            time.sleep(sleep_seconds)
    if last_exc is not None:
        raise last_exc
    raise ValueError(f"Unable to fetch option chain for {expiration}.")


def _options_with_retry(ticker_obj) -> list:
    retries = max(0, _get_int_env("YF_OPTION_RETRIES", 2))
    backoff = max(0.0, _get_float_env("YF_OPTION_RETRY_BACKOFF_SECONDS", 1.0))
    last_exc = None
    options = None
    for attempt in range(retries + 1):
        _throttle_yfinance()
        try:
            options = ticker_obj.options
        except Exception as exc:  # pragma: no cover - exercised in integration
            last_exc = exc
            options = None
            if _is_rate_limit_error(exc):
                _apply_rate_limit_cooldown()
        if options:
            return list(options)
        if attempt < retries:
            sleep_seconds = backoff * (2**attempt)
            if last_exc is not None and _is_rate_limit_error(last_exc):
                cooldown = max(0.0, _get_float_env("YF_RATE_LIMIT_COOLDOWN_SECONDS", 5.0))
                sleep_seconds = max(sleep_seconds, cooldown)
            time.sleep(sleep_seconds)
    if options is None and last_exc is not None:
        raise last_exc
    return list(options or [])


def _history_with_throttle(ticker_obj, period: str):
    _throttle_yfinance()
    return ticker_obj.history(period=period)


def fetch_stock_data(
    symbols,
    period="4mo",
    interval="1d",
    require_ohlc: bool = True,
    threads: bool = True,
):
    if isinstance(symbols, str):
        symbols = [symbols]

    upper_symbols = [sym.upper() for sym in symbols]
    threads = threads and len(upper_symbols) > 1

    try:
        raw_data = yf.download(
            tickers=" ".join(upper_symbols),
            period=period,
            interval=interval,
            group_by="ticker",
            auto_adjust=False,
            threads=threads,
            progress=False,
            timeout=10,
        )
    except Exception:
        raw_data = pd.DataFrame()
    if len(upper_symbols) == 1 and isinstance(raw_data, pd.DataFrame) and raw_data.empty:
        ticker = yf.Ticker(upper_symbols[0])
        raw_data = ticker.history(
            period=period,
            interval=interval,
            auto_adjust=False,
        )

    data_dict = {}
    is_single_frame = (
        isinstance(raw_data, pd.DataFrame)
        and not isinstance(raw_data.columns, pd.MultiIndex)
        and len(upper_symbols) == 1
    )

    for original_sym, upper_sym in zip(symbols, upper_symbols):
        try:
            if isinstance(raw_data, dict):
                ticker_df = raw_data[upper_sym].copy()
            elif is_single_frame:
                ticker_df = raw_data.copy()
            else:
                ticker_df = raw_data[upper_sym].copy()
        except KeyError:
            data_dict[original_sym] = pd.DataFrame()
            continue

        ticker_df.reset_index(inplace=True)

        if "Date" in ticker_df.columns:
            date_col = "Date"
        elif "Datetime" in ticker_df.columns:
            date_col = "Datetime"
        else:
            date_col = ticker_df.columns[0]

        rename_dict = {
            date_col: "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Volume": "volume",
        }
        close_source = None
        if "Adj Close" in ticker_df.columns:
            adj_close = ticker_df["Adj Close"]
            if adj_close.notna().any():
                close_source = "Adj Close"
        if "Close" in ticker_df.columns:
            close_col = ticker_df["Close"]
            if close_source is None:
                close_source = "Close"
            elif close_col.notna().sum() > ticker_df[close_source].notna().sum():
                close_source = "Close"
        if close_source:
            rename_dict[close_source] = "close"

        ticker_df.rename(columns=rename_dict, inplace=True)
        ticker_df["date"] = pd.to_datetime(ticker_df["date"])
        ticker_df.sort_values("date", inplace=True)
        ticker_df.reset_index(drop=True, inplace=True)

        ticker_df.replace({None: np.nan, np.inf: np.nan, -np.inf: np.nan}, inplace=True)
        if "close" in ticker_df.columns:
            ticker_df["close"] = pd.to_numeric(ticker_df["close"], errors="coerce")
        if require_ohlc:
            required_cols = [col for col in ("open", "high", "low", "close") if col in ticker_df.columns]
        else:
            required_cols = ["close"] if "close" in ticker_df.columns else []
        if required_cols:
            ticker_df.dropna(axis=0, how="any", subset=required_cols, inplace=True)
        else:
            ticker_df.dropna(axis=0, how="any", inplace=True)
        ticker_df.reset_index(drop=True, inplace=True)

        data_dict[original_sym] = ticker_df

    return data_dict


def fetch_stock_option_data(
    ticker: str,
    expiration: str = None,
    all_expirations: bool = False,
    option_type: str = None,
):
    """
    Fetch option chains plus the latest stock price.

    Pseudocode:
    1) Load ticker history to estimate latest price.
    2) If expiration is provided, fetch that chain.
    3) Otherwise fetch all expirations or the first available chain.
    """
    ticker_obj = yf.Ticker(ticker)
    latest_price = None

    if expiration:
        chain = _option_chain_with_retry(ticker_obj, expiration)
        latest_price = _price_from_underlying(getattr(chain, "underlying", None))
        if latest_price is None:
            stock_info = _history_with_throttle(ticker_obj, period="1d")
            if not stock_info.empty:
                latest_price = float(stock_info["Close"].iloc[-1])
        calls_df = chain.calls
        puts_df = chain.puts
        if option_type == "calls":
            return {"ticker": ticker, "stock_price": latest_price, "option_data": calls_df}
        if option_type == "puts":
            return {"ticker": ticker, "stock_price": latest_price, "option_data": puts_df}
        return {
            "ticker": ticker,
            "stock_price": latest_price,
            "option_data": {"calls": calls_df, "puts": puts_df},
        }

    if all_expirations:
        expirations = _options_with_retry(ticker_obj)
        all_data = {}
        for exp_date in expirations:
            chain = _option_chain_with_retry(ticker_obj, exp_date)
            if latest_price is None:
                latest_price = _price_from_underlying(getattr(chain, "underlying", None))
            calls_df = chain.calls
            puts_df = chain.puts
            if option_type == "calls":
                all_data[exp_date] = calls_df
            elif option_type == "puts":
                all_data[exp_date] = puts_df
            else:
                all_data[exp_date] = {"calls": calls_df, "puts": puts_df}
        if latest_price is None:
            stock_info = _history_with_throttle(ticker_obj, period="1d")
            if not stock_info.empty:
                latest_price = float(stock_info["Close"].iloc[-1])
        return {"ticker": ticker, "stock_price": latest_price, "option_data": all_data}

    available_exps = _options_with_retry(ticker_obj)
    if not available_exps:
        raise ValueError(f"No option expiration dates found for {ticker}.")

    first_exp = available_exps[0]
    chain = _option_chain_with_retry(ticker_obj, first_exp)
    latest_price = _price_from_underlying(getattr(chain, "underlying", None))
    if latest_price is None:
        stock_info = _history_with_throttle(ticker_obj, period="1d")
        if not stock_info.empty:
            latest_price = float(stock_info["Close"].iloc[-1])
    calls_df = chain.calls
    puts_df = chain.puts

    if option_type == "calls":
        return {"ticker": ticker, "stock_price": latest_price, "option_data": calls_df}
    if option_type == "puts":
        return {"ticker": ticker, "stock_price": latest_price, "option_data": puts_df}
    return {
        "ticker": ticker,
        "stock_price": latest_price,
        "option_data": {"calls": calls_df, "puts": puts_df},
    }


def _build_income_annual_from_yfinance(symbol: str) -> list:
    return build_income_annual_from_yfinance(symbol)
