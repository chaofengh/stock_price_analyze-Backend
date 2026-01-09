"""
data_fetcher_market.py
Purpose: fetch OHLCV data and option chains from yfinance.
"""
import numpy as np
import pandas as pd
import yfinance as yf

from .financials_yfinance import build_income_annual_from_yfinance


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
    stock_info = ticker_obj.history(period="1d")
    if not stock_info.empty:
        latest_price = float(stock_info["Close"].iloc[-1])
    else:
        latest_price = None

    if expiration:
        chain = ticker_obj.option_chain(expiration)
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
        expirations = ticker_obj.options
        all_data = {}
        for exp_date in expirations:
            chain = ticker_obj.option_chain(exp_date)
            calls_df = chain.calls
            puts_df = chain.puts
            if option_type == "calls":
                all_data[exp_date] = calls_df
            elif option_type == "puts":
                all_data[exp_date] = puts_df
            else:
                all_data[exp_date] = {"calls": calls_df, "puts": puts_df}
        return {"ticker": ticker, "stock_price": latest_price, "option_data": all_data}

    available_exps = ticker_obj.options
    if not available_exps:
        raise ValueError(f"No option expiration dates found for {ticker}.")

    first_exp = available_exps[0]
    chain = ticker_obj.option_chain(first_exp)
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
