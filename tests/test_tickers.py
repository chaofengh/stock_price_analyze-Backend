# tests/test_tickers.py
import json
import pandas as pd
from unittest.mock import patch

def test_delete_ticker_missing_field(client):
    """
    If 'ticker' is not in the JSON payload, a 400 is returned.
    """
    response = client.delete("/api/tickers", json={})
    assert response.status_code == 400
    data = response.get_json()
    assert "error" in data

def test_delete_ticker_success(client):
    """
    Test successful deletion with a mock removing the ticker from DB.
    """
    with patch("routes.tickers_routes.remove_ticker") as mock_remove:
        response = client.delete("/api/tickers", json={"ticker": "TSLA"})
        assert response.status_code == 200
        data = response.get_json()
        assert data["status"] == "deleted"
        mock_remove.assert_called_once_with("TSLA")

def test_get_tickers_success(client):
    """
    Mock DB call to return tickers, then mock fetch_stock_data for returning sample data.
    """
    fake_tickers = ["TSLA", "AAPL"]
    fake_data = {
        "TSLA": pd.DataFrame([{"date": "2023-01-01", "close": 100.0}]),
        "AAPL": pd.DataFrame([{"date": "2023-01-01", "close": 150.0}])
    }
    expected_output = {
        "TSLA": [{"date": "2023-01-01", "close": 100.0}],
        "AAPL": [{"date": "2023-01-01", "close": 150.0}]
    }

    with patch("routes.tickers_routes.get_all_tickers", return_value=fake_tickers), \
         patch("routes.tickers_routes.fetch_stock_data", return_value=fake_data):
        response = client.get("/api/tickers")
        assert response.status_code == 200
        data = response.get_json()
        assert data == expected_output

def test_add_ticker_single(client):
    """
    Test adding a single ticker in the request body.
    """
    with patch("routes.tickers_routes.insert_tickers") as mock_insert:
        response = client.post("/api/tickers", json={"ticker": "PLTR"})
        assert response.status_code == 200
        data = response.get_json()
        assert data["status"] == "success"
        mock_insert.assert_called_once_with(["PLTR"])

def test_add_ticker_multiple(client):
    """
    Test adding multiple tickers in the request body.
    """
    with patch("routes.tickers_routes.insert_tickers") as mock_insert:
        response = client.post("/api/tickers", json={"tickers": ["MSFT", "AMZN"]})
        assert response.status_code == 200
        data = response.get_json()
        assert data["status"] == "success"
        mock_insert.assert_called_once_with(["MSFT", "AMZN"])
