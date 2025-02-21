import os
import pandas as pd
from alpha_vantage.timeseries import TimeSeries
import yfinance as yf
import finnhub
import requests


alpha_vantage_api_key = os.environ.get("alpha_vantage_api_key")
finnhub_api_key = os.environ.get("finnhub_api_key")
finnhub_client = finnhub.Client(api_key=finnhub_api_key)


def fetch_stock_data(symbols, period="6mo", interval="1d"):
    if isinstance(symbols, str):
        symbols = [symbols]

    # Normalize all symbols to uppercase
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

        # Build a rename dict
        rename_dict = {
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Volume": "volume"
        }
        if "Adj Close" in ticker_df.columns:
            rename_dict["Adj Close"] = "close"
        elif "Close" in ticker_df.columns:
            rename_dict["Close"] = "close"

        ticker_df.rename(columns=rename_dict, inplace=True)

        # Clean up index and dates
        ticker_df["date"] = pd.to_datetime(ticker_df["date"])
        ticker_df.sort_values("date", inplace=True)
        ticker_df.reset_index(drop=True, inplace=True)

        # Store the cleaned DataFrame in data_dict under the ORIGINAL symbol 
        # (so data_dict has the same key the user typed in)
        data_dict[original_sym] = ticker_df

    return data_dict



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
    
def fetch_income_statement(symbol: str) -> dict:
    """
    Fetches the income statement data for the given symbol using the Alpha Vantage API.
    Returns a dictionary containing the symbol and only the last three annual reports.
    """
    if not alpha_vantage_api_key:
        raise ValueError("Missing 'alpha_vantage_api_key' in environment")
    
    url = f"https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={symbol}&apikey={alpha_vantage_api_key}"
    response = requests.get(url)
    
    if response.status_code != 200:
        raise Exception(f"Error fetching income statement data: {response.status_code}")
    
    data = response.json()
    
    # Remove the quarterlyReports key if it exists.
    data.pop("quarterlyReports", None)
    
    # Filter and sort annualReports to keep only the last three years.
    if "annualReports" in data:
        data["annualReports"] = sorted(
            data["annualReports"],
            key=lambda x: x.get("fiscalDateEnding", ""),
            reverse=True
        )[:3]
    
    return data

