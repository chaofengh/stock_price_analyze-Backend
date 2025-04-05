# tests/test_tickers.py
import pandas as pd
from unittest.mock import patch, call

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
    # Patch the correct function: remove_ticker_from_user_list
    with patch("routes.tickers_routes.remove_ticker_from_user_list") as mock_remove:
        response = client.delete("/api/tickers", json={"user_id": 123, "ticker": "TSLA"})
        assert response.status_code == 200
        data = response.get_json()
        assert data["status"] == "deleted"
        mock_remove.assert_called_once_with(123, "TSLA")

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
    # Patch the correct function: add_ticker_to_user_list
    with patch("routes.tickers_routes.add_ticker_to_user_list") as mock_add:
        response = client.post("/api/tickers", json={"user_id": 123, "ticker": "PLTR"})
        assert response.status_code == 200
        data = response.get_json()
        assert data["status"] == "success"
        mock_add.assert_called_once_with(123, "PLTR")

def test_add_ticker_multiple(client):
    """
    Test adding multiple tickers in the request body.
    """
    # Patch the correct function: add_ticker_to_user_list
    with patch("routes.tickers_routes.add_ticker_to_user_list") as mock_add:
        response = client.post("/api/tickers", json={"user_id": 123, "tickers": ["MSFT", "AMZN"]})
        assert response.status_code == 200
        data = response.get_json()
        assert data["status"] == "success"
        # Expect two calls, one for each ticker
        expected_calls = [call(123, "MSFT"), call(123, "AMZN")]
        assert mock_add.call_count == 2
        mock_add.assert_has_calls(expected_calls, any_order=False)
