# tests/test_data_fetcher_fundamentals_api.py

from unittest.mock import Mock

import pytest

import analysis.data_fetcher_fundamentals as fundamentals


def test_fetch_stock_fundamentals_uses_alt_symbol(monkeypatch):
    mock_load = Mock(
        side_effect=[
            {},
            {"trailingPE": 12.0},
        ]
    )
    mock_empty = Mock(side_effect=lambda payload: not payload)
    monkeypatch.setattr(fundamentals, "load_fundamentals", mock_load)
    monkeypatch.setattr(fundamentals, "is_empty_fundamentals", mock_empty)

    result = fundamentals.fetch_stock_fundamentals("brk.b")
    assert result["trailingPE"] == 12.0
    assert mock_load.call_args_list[0].args[0] == "BRK.B"
    assert mock_load.call_args_list[1].args[0] == "BRK-B"


def test_fetch_stock_fundamentals_returns_empty_when_missing(monkeypatch):
    mock_load = Mock(side_effect=[{}, {}])
    mock_empty = Mock(side_effect=lambda payload: not payload)
    monkeypatch.setattr(fundamentals, "load_fundamentals", mock_load)
    monkeypatch.setattr(fundamentals, "is_empty_fundamentals", mock_empty)

    result = fundamentals.fetch_stock_fundamentals("AAPL")
    assert result == {}


def test_fetch_peers_handles_errors(monkeypatch):
    mock_client = Mock()
    mock_client.company_peers.side_effect = RuntimeError("boom")
    monkeypatch.setattr(fundamentals, "finnhub_client", mock_client)
    assert fundamentals.fetch_peers("AAPL") == []

    mock_client.company_peers.side_effect = None
    mock_client.company_peers.return_value = "bad"
    assert fundamentals.fetch_peers("AAPL") == []
