import os
import pandas as pd
from alpha_vantage.timeseries import TimeSeries
import yfinance as yf
import finnhub
import requests
from dotenv import load_dotenv
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
        if "Adj Close" in ticker_df.columns:
            rename_dict["Adj Close"] = "close"
        elif "Close" in ticker_df.columns:
            rename_dict["Close"] = "close"

        ticker_df.rename(columns=rename_dict, inplace=True)
        ticker_df["date"] = pd.to_datetime(ticker_df["date"])
        ticker_df.sort_values("date", inplace=True)
        ticker_df.reset_index(drop=True, inplace=True)

        data_dict[original_sym] = ticker_df
    print(data_dict)
    return data_dict


fetch_stock_data('TSLA','1d','1m')