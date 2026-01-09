# tests/test_data_fetcher_financials.py

import json
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
    monkeypatch.setattr(financials, "get_financial_statement", lambda *_args: None)
    with pytest.raises(ValueError):
        financials.fetch_financials("AAPL")


def test_fetch_financials_uses_db_when_available(monkeypatch):
    db_payload = {"symbol": "AAPL", "annualReports": [{"fiscalDateEnding": "2023-12-31"}]}
    monkeypatch.setattr(financials, "get_financial_statement", lambda *_args: db_payload)
    result = financials.fetch_financials("AAPL", statements="income_statement")
    assert result["income_statement"]["annualReports"] == db_payload["annualReports"]


def test_fetch_financials_sorts_truncates_and_computes(monkeypatch):
    payload = _load_fixture("alpha_vantage_income_statement.json")
    monkeypatch.setattr(financials, "alpha_vantage_api_key", "test")
    monkeypatch.setattr(financials, "get_financial_statement", lambda *_args: None)
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
    monkeypatch.setattr(financials, "get_financial_statement", lambda *_args: None)
    monkeypatch.setattr(financials.requests, "get", lambda _url, timeout=8: _FakeResponse(payload))
    with pytest.raises(ValueError):
        financials.fetch_financials("AAPL", statements="income_statement")


def test_fetch_financials_invalid_statement(monkeypatch):
    monkeypatch.setattr(financials, "alpha_vantage_api_key", "test")
    with pytest.raises(ValueError):
        financials.fetch_financials("AAPL", statements="not_a_real_statement")
