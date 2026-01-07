# tests/test_data_fetcher_financials.py

import json
from pathlib import Path

import pytest

import analysis.data_fetcher_financials as financials
from utils.ttl_cache import TTLCache


FIXTURE_DIR = Path(__file__).with_name("fixtures") / "data_fetcher"


class _FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload


@pytest.fixture
def reset_financials_state(monkeypatch):
    monkeypatch.setattr(financials, "_FINANCIALS_CACHE", TTLCache(ttl_seconds=60, max_size=10))
    monkeypatch.setattr(
        financials, "_FINANCIALS_EMPTY_CACHE", TTLCache(ttl_seconds=60, max_size=10)
    )
    financials._AV_CALL_TIMES.clear()
    financials._AV_DISABLED_UNTIL = 0.0
    monkeypatch.setattr(financials, "_AV_RATE_WINDOW_SECONDS", 60)
    monkeypatch.setattr(financials, "_AV_RATE_MAX_CALLS", 2)
    monkeypatch.setattr(financials, "_AV_DISABLE_SECONDS", 120)


def _load_fixture(name):
    return json.loads((FIXTURE_DIR / name).read_text())


def test_fetch_financials_requires_api_key(monkeypatch, reset_financials_state):
    monkeypatch.setattr(financials, "alpha_vantage_api_key", None)
    with pytest.raises(ValueError):
        financials.fetch_financials("AAPL")


def test_fetch_financials_sorts_truncates_and_computes(monkeypatch, reset_financials_state):
    payload = _load_fixture("alpha_vantage_income_statement.json")
    monkeypatch.setattr(financials, "alpha_vantage_api_key", "test")
    monkeypatch.setattr(financials, "_alpha_vantage_allow_request", lambda: True)
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


def test_fetch_financials_income_fallback_uses_yfinance(monkeypatch, reset_financials_state):
    payload = {"Error Message": "bad call"}
    fallback = [{"fiscalDateEnding": "2023-12-31", "totalRevenue": "123"}]
    monkeypatch.setattr(financials, "alpha_vantage_api_key", "test")
    monkeypatch.setattr(financials, "_alpha_vantage_allow_request", lambda: True)
    monkeypatch.setattr(financials.requests, "get", lambda _url, timeout=8: _FakeResponse(payload))
    monkeypatch.setattr(financials, "_build_income_annual_from_yfinance", lambda _s: fallback)

    result = financials.fetch_financials("AAPL", statements="income_statement")
    data = result["income_statement"]
    assert data["annualReports"] == fallback
    assert "Error Message" not in data


def test_fetch_financials_invalid_statement(monkeypatch, reset_financials_state):
    monkeypatch.setattr(financials, "alpha_vantage_api_key", "test")
    with pytest.raises(ValueError):
        financials.fetch_financials("AAPL", statements="not_a_real_statement")


def test_alpha_vantage_rate_limits(monkeypatch, reset_financials_state):
    times = iter([0.0, 1.0, 2.0])
    monkeypatch.setattr(financials.time, "time", lambda: next(times))
    assert financials._alpha_vantage_allow_request() is True
    assert financials._alpha_vantage_allow_request() is True
    assert financials._alpha_vantage_allow_request() is False

    monkeypatch.setattr(financials.time, "time", lambda: 100.0)
    financials._alpha_vantage_disable_for_window()
    monkeypatch.setattr(financials.time, "time", lambda: 101.0)
    assert financials._alpha_vantage_allow_request() is False


def test_is_alpha_vantage_rate_limit_payload():
    assert financials._is_alpha_vantage_rate_limit({"Note": "API call frequency exceeded"}) is True
    assert financials._is_alpha_vantage_rate_limit({"Information": "Call frequency reached"}) is True
    assert financials._is_alpha_vantage_rate_limit({"ok": True}) is False
