# tests/test_data_fetcher_financials.py

import json
from datetime import datetime
from pathlib import Path

import pytest

import analysis.data_fetcher_financials as financials


FIXTURE_DIR = Path(__file__).with_name("fixtures") / "data_fetcher"


class _FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload


def _load_fixture(name):
    return json.loads((FIXTURE_DIR / name).read_text())


def test_fetch_financials_requires_api_key(monkeypatch):
    monkeypatch.setattr(financials, "alpha_vantage_api_key", None)
    monkeypatch.setattr(financials, "get_financial_statement", lambda *_args, **_kwargs: None)
    with pytest.raises(ValueError):
        financials.fetch_financials("AAPL")


def test_fetch_financials_uses_db_when_available(monkeypatch):
    db_payload = {"symbol": "AAPL", "quarterlyReports": [{"fiscalDateEnding": "2024-06-30"}]}
    monkeypatch.setattr(financials, "_utc_today", lambda: datetime(2024, 8, 1).date())
    monkeypatch.setattr(financials, "alpha_vantage_api_key", None)
    monkeypatch.setattr(
        financials,
        "get_financial_statement",
        lambda *_args, **_kwargs: db_payload,
    )
    result = financials.fetch_financials("AAPL", statements="income_statement")
    assert result["income_statement"]["quarterlyReports"] == db_payload["quarterlyReports"]


def test_fetch_financials_sorts_truncates_and_computes(monkeypatch):
    payload = _load_fixture("alpha_vantage_income_statement.json")
    monkeypatch.setattr(financials, "alpha_vantage_api_key", "test")
    monkeypatch.setattr(financials, "get_financial_statement", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        financials.requests, "get", lambda _url, timeout=8: _FakeResponse(payload)
    )

    result = financials.fetch_financials("aapl", statements="income_statement")
    data = result["income_statement"]
    assert data["symbol"] == "AAPL"
    assert len(data["quarterlyReports"]) == 12
    assert data["annualReports"]
    assert data["annualReports"][0]["fiscalDateEnding"] == "2023-12-31"
    assert data["partialYearReports"]


def test_fetch_financials_alpha_error_raises(monkeypatch):
    payload = {"Error Message": "bad call"}
    monkeypatch.setattr(financials, "alpha_vantage_api_key", "test")
    monkeypatch.setattr(financials, "get_financial_statement", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(financials.requests, "get", lambda _url, timeout=8: _FakeResponse(payload))
    with pytest.raises(ValueError):
        financials.fetch_financials("AAPL", statements="income_statement")


def test_fetch_financials_refreshes_when_stale(monkeypatch):
    payload = _load_fixture("alpha_vantage_income_statement.json")
    db_payload = {"symbol": "AAPL", "quarterlyReports": [{"fiscalDateEnding": "2024-03-31"}]}
    monkeypatch.setattr(financials, "_utc_today", lambda: datetime(2024, 10, 15).date())
    monkeypatch.setattr(financials, "alpha_vantage_api_key", "test")
    monkeypatch.setattr(
        financials,
        "get_financial_statement",
        lambda *_args, **_kwargs: db_payload,
    )
    monkeypatch.setattr(
        financials.requests, "get", lambda _url, timeout=8: _FakeResponse(payload)
    )
    captured = {}

    def _capture_upsert(symbol, statement_type, payload, source=None):
        captured["symbol"] = symbol
        captured["statement_type"] = statement_type
        captured["payload"] = payload
        captured["source"] = source

    monkeypatch.setattr(financials, "upsert_financial_statement", _capture_upsert)

    result = financials.fetch_financials("AAPL", statements="income_statement")
    data = result["income_statement"]
    assert data["symbol"] == "AAPL"
    assert captured["statement_type"] == "income_statement"
    assert captured["source"] == "alpha_vantage"
    assert data["quarterlyReports"][0]["fiscalDateEnding"] == "2024-06-30"


def test_fetch_financials_invalid_statement(monkeypatch):
    monkeypatch.setattr(financials, "alpha_vantage_api_key", "test")
    with pytest.raises(ValueError):
        financials.fetch_financials("AAPL", statements="not_a_real_statement")
