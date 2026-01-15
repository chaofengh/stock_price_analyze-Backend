from datetime import datetime, timezone

import analysis.financials_yfinance as financials_yfinance


class _FakeTicker:
    def __init__(self, info):
        self._info = info

    def get_info(self):
        return self._info

    @property
    def info(self):
        return self._info


def _to_epoch(dt):
    return int(dt.replace(tzinfo=timezone.utc).timestamp())


def test_get_fiscal_quarter_info_uses_fiscal_year_end(monkeypatch):
    info = {
        "mostRecentQuarter": _to_epoch(datetime(2025, 11, 30)),
        "lastFiscalYearEnd": _to_epoch(datetime(2025, 5, 31)),
    }
    monkeypatch.setattr(financials_yfinance.yf, "Ticker", lambda _symbol: _FakeTicker(info))

    result = financials_yfinance.get_fiscal_quarter_info("ORCL")

    assert result["mostRecentQuarter"] == "2025-11-30"
    assert result["lastFiscalYearEnd"] == "2025-05-31"
    assert result["mostRecentQuarterLabel"] == "Q2"


def test_get_fiscal_quarter_info_falls_back_to_calendar(monkeypatch):
    info = {"mostRecentQuarter": _to_epoch(datetime(2024, 6, 30))}
    monkeypatch.setattr(financials_yfinance.yf, "Ticker", lambda _symbol: _FakeTicker(info))

    result = financials_yfinance.get_fiscal_quarter_info("AAPL")

    assert result["mostRecentQuarter"] == "2024-06-30"
    assert result["mostRecentQuarterLabel"] == "Q2"
