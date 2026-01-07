# tests/test_data_fetcher_market.py

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

import analysis.data_fetcher_market as market


class _FakeTicker:
    def __init__(self, financials, options, chain, history_df):
        self.financials = financials
        self.options = options
        self._chain = chain
        self._history = history_df

    def option_chain(self, expiration):
        return self._chain

    def history(self, period="1d"):
        return self._history


def test_build_income_annual_from_yfinance_maps_fields(monkeypatch):
    financials = pd.DataFrame(
        {
            pd.Timestamp("2023-12-31"): [1000, 600, 200, 150],
            pd.Timestamp("2022-12-31"): [900, 540, 180, 140],
        },
        index=["Total Revenue", "Gross Profit", "Operating Income", "Net Income"],
    )
    fake_ticker = _FakeTicker(financials, [], None, pd.DataFrame())
    monkeypatch.setattr(market.yf, "Ticker", lambda symbol: fake_ticker)

    reports = market._build_income_annual_from_yfinance("AAPL")
    assert len(reports) == 2
    assert reports[0]["fiscalDateEnding"] == "2023-12-31"
    assert reports[0]["totalRevenue"] == "1000"
    assert reports[0]["operatingIncome"] == "200"
    assert reports[0]["netIncome"] == "150"


def test_fetch_stock_data_handles_datetime_and_drops_invalid(monkeypatch):
    idx = pd.to_datetime(["2024-01-02", "2024-01-03"])
    df = pd.DataFrame(
        {
            "Open": [100, np.nan],
            "High": [105, 106],
            "Low": [99, 100],
            "Close": [104, 105],
            "Adj Close": [104, 105],
            "Volume": [1000, 1100],
        },
        index=idx,
    )
    df.index.name = "Datetime"
    monkeypatch.setattr(market.yf, "download", lambda **_kwargs: {"MSFT": df})

    result = market.fetch_stock_data(["msft"], period="1d", interval="1m")
    out = result["msft"]
    assert len(out) == 1
    assert set(["date", "open", "high", "low", "close", "volume"]).issubset(out.columns)
    assert out["date"].dtype.kind == "M"


def test_fetch_stock_option_data_variants(monkeypatch):
    chain = SimpleNamespace(
        calls=pd.DataFrame({"contractSymbol": ["CALL1"], "strike": [100]}),
        puts=pd.DataFrame({"contractSymbol": ["PUT1"], "strike": [100]}),
    )
    history_df = pd.DataFrame({"Close": [123.45]})
    fake_ticker = _FakeTicker(financials=pd.DataFrame(), options=["2024-10-18"], chain=chain, history_df=history_df)
    monkeypatch.setattr(market.yf, "Ticker", lambda symbol: fake_ticker)

    result_calls = market.fetch_stock_option_data("AAPL", expiration="2024-10-18", option_type="calls")
    assert result_calls["ticker"] == "AAPL"
    assert result_calls["stock_price"] == 123.45
    assert result_calls["option_data"].equals(chain.calls)

    result_all = market.fetch_stock_option_data("AAPL", all_expirations=True)
    assert "2024-10-18" in result_all["option_data"]
    assert "calls" in result_all["option_data"]["2024-10-18"]

    result_default = market.fetch_stock_option_data("AAPL")
    assert set(result_default["option_data"].keys()) == {"calls", "puts"}
