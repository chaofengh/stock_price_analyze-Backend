# tests/test_fundamentals.py

import pytest
from unittest.mock import patch
from analysis.fundamentals import (
    get_fundamentals, 
    get_peers, 
    get_peers_fundamentals, 
    compute_peer_metric_avg, 
    compare_metric
)

@patch('analysis.fundamentals.fetch_stock_fundamentals')
def test_get_fundamentals(mock_fetch):
    mock_fetch.return_value = {"trailingPE": 10.0, "beta": 1.2}
    result = get_fundamentals("FAKE")
    assert result['trailingPE'] == 10.0
    assert result['beta'] == 1.2

@patch('analysis.fundamentals.fetch_peers')
def test_get_peers(mock_peers):
    mock_peers.return_value = ["SYM1", "SYM2"]
    peers = get_peers("FAKE")
    assert peers == ["SYM1", "SYM2"]

def test_compute_peer_metric_avg():
    peers_data = {
        "SYM1": {"trailingPE": 8.0},
        "SYM2": {"trailingPE": 12.0},
        "SYM3": {"trailingPE": None},
    }
    avg = compute_peer_metric_avg(peers_data, "trailingPE")
    assert avg == 10.0

def test_compare_metric():
    assert compare_metric(12, 10, "Trailing PE") == "Trailing PE is above peer average (12.00 vs. 10.00)."
    assert compare_metric(8, 10, "Trailing PE") == "Trailing PE is below (or near) peer average (8.00 vs. 10.00)."
    assert "No peer comparison" in compare_metric(None, 10, "Trailing PE")
