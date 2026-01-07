"""
Record live API responses for data_fetcher tests.

Usage (from stock_price_analyze_backend):
  python tests/fixtures/record_data_fetcher_fixtures.py --symbol AAPL --period 1mo --interval 1d

Environment:
  alpha_vantage_api_key  Alpha Vantage API key (required for Alpha Vantage fixtures)
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import requests
import yfinance as yf

FIXTURE_DIR = Path(__file__).resolve().parent / "data_fetcher"


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def _write_csv(path: Path, df) -> None:
    df.to_csv(path, index=True)


def record_alpha_vantage(symbol: str, api_key: str) -> None:
    fixtures = {
        "INCOME_STATEMENT": "alpha_vantage_income_statement.json",
        "BALANCE_SHEET": "alpha_vantage_balance_sheet.json",
        "CASH_FLOW": "alpha_vantage_cash_flow.json",
    }
    for function_name, filename in fixtures.items():
        url = (
            "https://www.alphavantage.co/query"
            f"?function={function_name}&symbol={symbol}&apikey={api_key}"
        )
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        _write_json(FIXTURE_DIR / filename, data)


def record_yfinance_download(symbol: str, period: str, interval: str) -> None:
    df = yf.download(
        tickers=symbol,
        period=period,
        interval=interval,
        group_by="ticker",
        auto_adjust=False,
        threads=False,
    )
    _write_csv(FIXTURE_DIR / f"yfinance_download_{symbol}.csv", df)


def record_yfinance_option_chain(symbol: str) -> None:
    ticker = yf.Ticker(symbol)
    if not ticker.options:
        return
    chain = ticker.option_chain(ticker.options[0])
    payload = {
        "ticker": symbol,
        "expiration": ticker.options[0],
        "calls": chain.calls.to_dict(orient="list"),
        "puts": chain.puts.to_dict(orient="list"),
    }
    _write_json(FIXTURE_DIR / f"yfinance_option_chain_{symbol}.json", payload)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="AAPL")
    parser.add_argument("--period", default="1mo")
    parser.add_argument("--interval", default="1d")
    args = parser.parse_args()

    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)

    api_key = os.environ.get("alpha_vantage_api_key")
    if api_key:
        record_alpha_vantage(args.symbol, api_key)
    else:
        print("Skipping Alpha Vantage fixtures (missing alpha_vantage_api_key).")

    record_yfinance_download(args.symbol, args.period, args.interval)
    record_yfinance_option_chain(args.symbol)


if __name__ == "__main__":
    main()
