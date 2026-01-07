# tests/test_data_fetcher_fundamentals_loader.py

from unittest.mock import Mock

import pandas as pd
import pytest

import analysis.data_fetcher_fundamentals_loader as loader


class _FakeTicker:
    def __init__(self):
        self.info = {"trailingPE": 10.0}
        self.fast_info = {"lastPrice": 100.0}
        self.financials = pd.DataFrame({"2023-12-31": [100]}, index=["Total Revenue"])
        self.balance_sheet = pd.DataFrame({"2023-12-31": [200]}, index=["Total Assets"])
        self.cashflow = pd.DataFrame({"2023-12-31": [300]}, index=["Operating Cash Flow"])
        self.quarterly_financials = pd.DataFrame(
            {"2024-03-31": [25]}, index=["Total Revenue"]
        )
        self.quarterly_balance_sheet = pd.DataFrame(
            {"2024-03-31": [210]}, index=["Total Assets"]
        )
        self.quarterly_cashflow = pd.DataFrame(
            {"2024-03-31": [80]}, index=["Operating Cash Flow"]
        )


def test_load_fundamentals_calls_extract_and_alpha(monkeypatch):
    fake_ticker = _FakeTicker()
    monkeypatch.setattr(loader.yf, "Ticker", lambda symbol: fake_ticker)

    mock_fetch_financials = Mock(return_value={"income_statement": {"annualReports": []}})
    monkeypatch.setattr(loader, "fetch_financials", mock_fetch_financials)

    mock_extract = Mock(return_value={"ok": True})
    monkeypatch.setattr(loader, "extract_fundamentals", mock_extract)

    result = loader.load_fundamentals("AAPL", include_alpha=True)
    assert result == {"ok": True}
    mock_fetch_financials.assert_called_once()
    args, kwargs = mock_extract.call_args
    assert args[0]["trailingPE"] == 10.0
    assert args[1]["lastPrice"] == 100.0
    assert "alpha_financials" in kwargs["statements"]


def test_load_fundamentals_skips_alpha_when_disabled(monkeypatch):
    fake_ticker = _FakeTicker()
    monkeypatch.setattr(loader.yf, "Ticker", lambda symbol: fake_ticker)

    mock_fetch_financials = Mock()
    monkeypatch.setattr(loader, "fetch_financials", mock_fetch_financials)

    mock_extract = Mock(return_value={"ok": True})
    monkeypatch.setattr(loader, "extract_fundamentals", mock_extract)

    result = loader.load_fundamentals("AAPL", include_alpha=False)
    assert result == {"ok": True}
    mock_fetch_financials.assert_not_called()


def test_load_fundamentals_returns_empty_on_failure(monkeypatch):
    def _boom(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(loader.yf, "Ticker", _boom)
    assert loader.load_fundamentals("AAPL") == {}
