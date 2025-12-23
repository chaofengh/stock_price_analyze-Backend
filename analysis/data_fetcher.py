#analysis.data_fetcher.py
import os
from collections import defaultdict
from decimal import Decimal, InvalidOperation

import pandas as pd
from alpha_vantage.timeseries import TimeSeries
import yfinance as yf
import finnhub
import requests
from dotenv import load_dotenv
import numpy as np
load_dotenv()

alpha_vantage_api_key = os.environ.get("alpha_vantage_api_key")
finnhub_api_key = os.environ.get("finnhub_api_key")
finnhub_client = finnhub.Client(api_key=finnhub_api_key)



def fetch_stock_data(symbols, period="4mo", interval="1d"):
    if isinstance(symbols, str):
        symbols = [symbols]

    # Normalize symbols to uppercase
    upper_symbols = [sym.upper() for sym in symbols]

    raw_data = yf.download(
        tickers=" ".join(upper_symbols),
        period=period,
        interval=interval,
        group_by="ticker",
        auto_adjust=False,
        threads=True
    )

    data_dict = {}

    for original_sym, upper_sym in zip(symbols, upper_symbols):
        try:
            ticker_df = raw_data[upper_sym].copy()
        except KeyError:
            print(f"Ticker {upper_sym} not found in raw_data for user symbol {original_sym}")
            continue

        ticker_df.reset_index(inplace=True)

        # Check if the datetime column is labeled as 'Date' or 'Datetime'
        if "Date" in ticker_df.columns:
            date_col = "Date"
        elif "Datetime" in ticker_df.columns:
            date_col = "Datetime"
        else:
            date_col = ticker_df.columns[0]  # fallback if needed

        rename_dict = {
            date_col: "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Volume": "volume"
        }
        # Decide how to handle 'Close' vs 'Adj Close'
        if "Adj Close" in ticker_df.columns:
            rename_dict["Adj Close"] = "close"
        elif "Close" in ticker_df.columns:
            rename_dict["Close"] = "close"

        ticker_df.rename(columns=rename_dict, inplace=True)
        ticker_df["date"] = pd.to_datetime(ticker_df["date"])
        ticker_df.sort_values("date", inplace=True)
        ticker_df.reset_index(drop=True, inplace=True)

        # Convert None, +∞, and -∞ to np.nan so dropna() can detect them
        ticker_df.replace({None: np.nan, np.inf: np.nan, -np.inf: np.nan}, inplace=True)

        # Drop rows that contain any NaN
        ticker_df.dropna(axis=0, how='any', inplace=True)

        data_dict[original_sym] = ticker_df

    return data_dict


def fetch_stock_option_data(
    ticker: str,
    expiration: str = None,
    all_expirations: bool = False,
    option_type: str = None
):
    """
    Fetch option chain data (calls/puts) for a given ticker using yfinance,
    along with the latest trading price of the stock.

    :param ticker: The stock ticker symbol (e.g., 'AAPL', 'TSLA').
    :param expiration: A specific expiration date in 'YYYY-MM-DD' format.
                       If provided, only that date's options are fetched.
    :param all_expirations: If True, fetch option chains for all available expirations.
                           (Ignored if 'expiration' is given.)
    :param option_type: 'calls', 'puts', or None.
                       - If 'calls', only return the calls DataFrame.
                       - If 'puts', only return the puts DataFrame.
                       - If None, return a dict {'calls': DataFrame, 'puts': DataFrame}.
    :return:
        {
          "ticker": <str>,
          "stock_price": <float or None>,
          "option_data": <DataFrame or dict of DataFrames>
        }
    """

    # Create the ticker object
    ticker_obj = yf.Ticker(ticker)

    # Attempt to get the latest trading price of the stock
    # This will return daily historical data for 1 day
    stock_info = ticker_obj.history(period='1d')
    if not stock_info.empty:
        # The 'Close' price on the last row is typically the latest trading price
        latest_price = float(stock_info['Close'].iloc[-1])
    else:
        # If we fail to fetch data, set price to None or handle as needed
        latest_price = None

    # 1) Single expiration date
    if expiration:
        chain = ticker_obj.option_chain(expiration)
        calls_df = chain.calls
        puts_df = chain.puts

        if option_type == 'calls':
            return {
                "ticker": ticker,
                "stock_price": latest_price,
                "option_data": calls_df
            }
        elif option_type == 'puts':
            return {
                "ticker": ticker,
                "stock_price": latest_price,
                "option_data": puts_df
            }
        else:
            return {
                "ticker": ticker,
                "stock_price": latest_price,
                "option_data": {
                    "calls": calls_df,
                    "puts": puts_df
                }
            }

    # 2) All expirations
    if all_expirations:
        expirations = ticker_obj.options  # list of dates as strings
        all_data = {}

        for exp_date in expirations:
            chain = ticker_obj.option_chain(exp_date)
            calls_df = chain.calls
            puts_df = chain.puts

            if option_type == 'calls':
                all_data[exp_date] = calls_df
            elif option_type == 'puts':
                all_data[exp_date] = puts_df
            else:
                all_data[exp_date] = {
                    "calls": calls_df,
                    "puts": puts_df
                }

        return {
            "ticker": ticker,
            "stock_price": latest_price,
            "option_data": all_data
        }

    # 3) If neither 'expiration' nor 'all_expirations' is set,
    #    fetch the first available expiration by default
    available_exps = ticker_obj.options
    if not available_exps:
        raise ValueError(f"No option expiration dates found for {ticker}.")

    first_exp = available_exps[0]
    chain = ticker_obj.option_chain(first_exp)
    calls_df = chain.calls
    puts_df = chain.puts

    if option_type == 'calls':
        return {
            "ticker": ticker,
            "stock_price": latest_price,
            "option_data": calls_df
        }
    elif option_type == 'puts':
        return {
            "ticker": ticker,
            "stock_price": latest_price,
            "option_data": puts_df
        }
    else:
        return {
            "ticker": ticker,
            "stock_price": latest_price,
            "option_data": {
                "calls": calls_df,
                "puts": puts_df
            }
        }




def fetch_stock_fundamentals(symbol):
    """
    Fetches stock fundamentals from Yahoo Finance.
    
    Returns a dictionary containing the following metrics:
      - trailingPE: Trailing Price-to-Earnings ratio.
      - forwardPE: Forward Price-to-Earnings ratio.
      - PEG: Forward PEG ratio computed as forwardPE divided by (earningsGrowth * 100).
      - PGI: Ratio-based PE Growth Index, computed as forwardPE / trailingPE.
      - trailingPEG: Trailing PEG ratio as reported by Yahoo.
      - dividendYield: Dividend yield.
      - beta: Stock beta.
      - marketCap: Market capitalization.
      - priceToBook: Price-to-book ratio.
      - forwardEPS: Forward Earnings Per Share.
      - trailingEPS: Trailing Earnings Per Share.
      - debtToEquity: Debt-to-Equity ratio.
    """
    ticker = yf.Ticker(symbol)
    info = ticker.info

    # Helper function to safely convert metrics to float
    def safe_float(key):
        try:
            value = info.get(key)
            return float(value) if value is not None else None
        except Exception:
            return None

    # Extract basic metrics
    trailing_pe = safe_float("trailingPE")
    forward_pe = safe_float("forwardPE")
    earnings_growth = safe_float("earningsGrowth")  # Expected as a decimal (e.g., 0.15 for 15%)

    # Compute PEG: forwardPE divided by (earningsGrowth * 100)
    if forward_pe is not None and earnings_growth is not None and earnings_growth != 0:
        PEG = forward_pe / (earnings_growth * 100)
    else:
        PEG = None

    # Compute PGI: ratio-based PE Growth Index (using forwardPE and trailingPE)
    if forward_pe is not None and trailing_pe is not None and trailing_pe != 0:
        PGI = forward_pe / trailing_pe
    else:
        PGI = None

    # Extract additional metrics
    trailingPEG    = safe_float("trailingPegRatio")
    dividendYield  = safe_float("dividendYield")
    beta           = safe_float("beta")
    marketCap      = safe_float("marketCap")
    priceToBook    = safe_float("priceToBook")
    forwardEPS     = safe_float("forwardEps")
    trailingEPS    = safe_float("trailingEps")
    debtToEquity   = safe_float("debtToEquity")

    return {
        "trailingPE": trailing_pe,
        "forwardPE": forward_pe,
        "PEG": PEG,
        "PGI": PGI,
        "trailingPEG": trailingPEG,
        "dividendYield": dividendYield,
        "beta": beta,
        "marketCap": marketCap,
        "priceToBook": priceToBook,
        "forwardEPS": forwardEPS,
        "trailingEPS": trailingEPS,
        "debtToEquity": debtToEquity,
    }

def fetch_peers(symbol: str) -> list:
    """
    Fetches a list of peer tickers for a given symbol using Finnhub's company_peers endpoint.
    Returns an empty list if none are found or if the symbol is invalid.
    """
    try:
        peers = finnhub_client.company_peers(symbol)
        if isinstance(peers, list):
            return peers
        return []
    except Exception:
        return []
    
def fetch_financials(symbol: str, statements=None) -> dict:
    """
    Fetches specified financial statement data for the given symbol using the Alpha Vantage API.

    Parameters:
        symbol (str): The stock ticker symbol.
        statements (str or list of str, optional): Specifies which financial statement(s) to fetch.
            Valid options are "income_statement", "balance_sheet", "cash_flow".
            If None, all three types will be fetched.
            You can also pass a single string (e.g., "income_statement") or a list of strings.
    
    Returns:
        dict: A dictionary where each key is one of the requested statement types. 
              Each value contains the filtered data with the last three annualReports 
              and the last eight quarterlyReports.
    
    Raises:
        ValueError: If the API key is missing or if an invalid statement type is provided.
        Exception: For any errors during the HTTP request.
    """
    if not alpha_vantage_api_key:
        raise ValueError("Missing 'alpha_vantage_api_key' in environment")
    
    # Mapping from our statement keys to the Alpha Vantage API function names.
    valid_types = {
        "income_statement": "INCOME_STATEMENT",
        "balance_sheet": "BALANCE_SHEET",
        "cash_flow": "CASH_FLOW"
    }
    
    # If no specific statement(s) are requested, fetch all types.
    if statements is None:
        requested_types = list(valid_types.keys())
    else:
        # Allow a single string to be passed.
        if isinstance(statements, str):
            requested_types = [statements]
        elif isinstance(statements, list):
            requested_types = statements
        else:
            raise ValueError("`statements` must be a string or a list of strings")
    
    # Validate requested statement types.
    for statement in requested_types:
        if statement not in valid_types:
            raise ValueError(f"Invalid statement type: {statement}. Valid options are: {list(valid_types.keys())}")
    
    financials = {}
    for statement in requested_types:
        function_name = valid_types[statement]
        url = f"https://www.alphavantage.co/query?function={function_name}&symbol={symbol}&apikey={alpha_vantage_api_key}"
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Error fetching {statement} data: {response.status_code}")
        data = response.json()
        
        # Filter and sort annualReports to keep only the last three entries.
        if "annualReports" in data:
            data["annualReports"] = sorted(
                data["annualReports"],
                key=lambda x: x.get("fiscalDateEnding", ""),
                reverse=True
            )[:3]
        
        # Filter and sort quarterlyReports to keep only the last twelve entries.
        if "quarterlyReports" in data:
            data["quarterlyReports"] = sorted(
                data["quarterlyReports"],
                key=lambda x: x.get("fiscalDateEnding", ""),
                reverse=True
            )[:12]

            partial_year = _compute_partial_year_reports(data["quarterlyReports"])
            if partial_year:
                data["partialYearReports"] = partial_year

        data["symbol"] = symbol
        financials[statement] = data
    return financials


def _compute_partial_year_reports(quarterly_reports):
    """Builds year-to-date style aggregates for the latest year and two prior years."""
    if not quarterly_reports:
        return []

    reports_with_dates = [
        rpt for rpt in quarterly_reports if rpt.get("fiscalDateEnding")
    ]
    if not reports_with_dates:
        return []

    latest_report = max(reports_with_dates, key=lambda r: r["fiscalDateEnding"])
    latest_date = latest_report.get("fiscalDateEnding", "")
    if len(latest_date) < 7:
        return []

    latest_year = latest_date[:4]
    latest_month = latest_date[5:7]
    latest_quarter = _month_to_quarter(latest_month)
    if latest_quarter is None or latest_quarter == 0:
        return []

    # Organize reports by year and quarter for quick lookups
    reports_by_year = defaultdict(dict)
    for report in quarterly_reports:
        date_str = report.get("fiscalDateEnding")
        if not date_str or len(date_str) < 7:
            continue
        year = date_str[:4]
        quarter = _month_to_quarter(date_str[5:7])
        if quarter is None:
            continue
        reports_by_year[year][quarter] = report

    years_to_build = [str(int(latest_year) - offset) for offset in range(3)]
    partial_sets = []

    for year in years_to_build:
        year_reports = reports_by_year.get(year)
        if not year_reports:
            continue

        selected = []
        for quarter in range(1, latest_quarter + 1):
            quarter_report = year_reports.get(quarter)
            if not quarter_report:
                selected = []
                break
            selected.append(quarter_report)

        if not selected:
            continue

        aggregated = _aggregate_quarter_reports(selected)
        aggregated["fiscalDateEnding"] = f"{year}-YTD-Q{latest_quarter}"
        aggregated["quarterRange"] = f"Q1-Q{latest_quarter}"
        aggregated["quarterCount"] = latest_quarter
        aggregated["quartersIncluded"] = [f"Q{i}" for i in range(1, latest_quarter + 1)]
        aggregated["year"] = year
        partial_sets.append(aggregated)

    # Return in chronological order (oldest first)
    return sorted(partial_sets, key=lambda x: x["year"])


def _aggregate_quarter_reports(reports):
    totals = {}
    currency = None

    for report in reports:
        if not currency:
            currency = report.get("reportedCurrency")
        for key, value in report.items():
            if key in {"fiscalDateEnding", "reportedCurrency"}:
                continue
            numeric_value = _safe_decimal(value)
            if numeric_value is None:
                continue
            totals[key] = totals.get(key, Decimal("0")) + numeric_value

    aggregated = {key: _decimal_to_string(val) for key, val in totals.items()}
    if currency:
        aggregated["reportedCurrency"] = currency
    return aggregated


def _month_to_quarter(month_str):
    try:
        month = int(month_str)
    except (TypeError, ValueError):
        return None
    if 1 <= month <= 3:
        return 1
    if 4 <= month <= 6:
        return 2
    if 7 <= month <= 9:
        return 3
    if 10 <= month <= 12:
        return 4
    return None


def _safe_decimal(value):
    if value in (None, "", "None"):
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _decimal_to_string(value):
    if value == value.to_integral():
        return str(int(value))
    return format(value, "f")
