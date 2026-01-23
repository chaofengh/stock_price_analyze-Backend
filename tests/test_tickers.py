# tests/test_tickers.py
from unittest.mock import patch, call
from utils.auth import AuthResult

def test_delete_ticker_missing_field(client):
    """
    If 'ticker' is not in the JSON payload, a 400 is returned.
    """
    with patch(
        "routes.tickers_routes.authenticate_bearer_token",
        return_value=AuthResult(user_id=123, payload={"user_id": 123}),
    ):
        response = client.delete(
            "/api/tickers",
            headers={"Authorization": "Bearer test"},
            json={},
        )
    assert response.status_code == 400
    data = response.get_json()
    assert "error" in data

def test_delete_ticker_success(client):
    """
    Test successful deletion with a mock removing the ticker from DB.
    """
    # Patch the correct function: remove_ticker_from_user_list
    with patch(
        "routes.tickers_routes.authenticate_bearer_token",
        return_value=AuthResult(user_id=123, payload={"user_id": 123}),
    ), patch("routes.tickers_routes.remove_ticker_from_user_list") as mock_remove:
        response = client.delete(
            "/api/tickers",
            headers={"Authorization": "Bearer test"},
            json={"ticker": "TSLA"},
        )
        assert response.status_code == 200
        data = response.get_json()
        assert data["status"] == "deleted"
        mock_remove.assert_called_once_with(123, "TSLA")

def test_get_tickers_success(client):
    """
    Mock DB call to return tickers, then mock cached price movement data.
    """
    fake_tickers = ["TSLA", "AAPL"]
    fake_data = {
        "TSLA": {"candles": [{"date": "2023-01-02", "close": 100.0}], "summary": {"previousClose": 98.0}},
        "AAPL": {"candles": [{"date": "2023-01-02", "close": 150.0}], "summary": {"previousClose": 148.0}},
    }
    expected_output = {
        "TSLA": {"candles": [{"date": "2023-01-02", "close": 100.0}], "summary": {"previousClose": 98.0}},
        "AAPL": {"candles": [{"date": "2023-01-02", "close": 150.0}], "summary": {"previousClose": 148.0}},
    }

    with patch(
        "routes.tickers_routes.authenticate_bearer_token",
        return_value=AuthResult(user_id=123, payload={"user_id": 123}),
    ), patch("routes.tickers_routes.get_all_tickers", return_value=fake_tickers), patch(
        "routes.tickers_routes.get_price_movement_data", return_value=fake_data
    ) as mock_cache:
        response = client.get("/api/tickers", headers={"Authorization": "Bearer test"})
        assert response.status_code == 200
        data = response.get_json()
        assert data == expected_output
        mock_cache.assert_called_once_with(fake_tickers)

def test_get_tickers_includes_all_user_tickers(client):
    """
    Ensure the response includes every ticker from the user's list,
    even if data is missing for some tickers.
    """
    fake_tickers = ["TSLA", "AAPL", "MSFT"]
    fake_data = {
        "TSLA": {"candles": [{"date": "2023-01-02", "close": 100.0}], "summary": {"previousClose": 98.0}},
        "AAPL": {"candles": [{"date": "2023-01-02", "close": 150.0}], "summary": {"previousClose": 148.0}},
    }

    with patch(
        "routes.tickers_routes.authenticate_bearer_token",
        return_value=AuthResult(user_id=123, payload={"user_id": 123}),
    ), patch("routes.tickers_routes.get_all_tickers", return_value=fake_tickers), patch(
        "routes.tickers_routes.get_price_movement_data", return_value=fake_data
    ):
        response = client.get("/api/tickers", headers={"Authorization": "Bearer test"})
        assert response.status_code == 200
        data = response.get_json()
        assert len(data) == len(fake_tickers)
        assert set(data.keys()) == set(fake_tickers)
        assert data["MSFT"]["candles"] == []
        assert data["MSFT"]["summary"]["previousClose"] is None

def test_add_ticker_single(client):
    """
    Test adding a single ticker in the request body.
    """
    # Patch the correct function: add_ticker_to_user_list
    with patch(
        "routes.tickers_routes.authenticate_bearer_token",
        return_value=AuthResult(user_id=123, payload={"user_id": 123}),
    ), patch("routes.tickers_routes.add_ticker_to_user_list") as mock_add:
        response = client.post(
            "/api/tickers",
            headers={"Authorization": "Bearer test"},
            json={"ticker": "PLTR"},
        )
        assert response.status_code == 200
        data = response.get_json()
        assert data["status"] == "success"
        mock_add.assert_called_once_with(123, "PLTR")

def test_add_ticker_multiple(client):
    """
    Test adding multiple tickers in the request body.
    """
    # Patch the correct function: add_ticker_to_user_list
    with patch(
        "routes.tickers_routes.authenticate_bearer_token",
        return_value=AuthResult(user_id=123, payload={"user_id": 123}),
    ), patch("routes.tickers_routes.add_ticker_to_user_list") as mock_add:
        response = client.post(
            "/api/tickers",
            headers={"Authorization": "Bearer test"},
            json={"tickers": ["MSFT", "AMZN"]},
        )
        assert response.status_code == 200
        data = response.get_json()
        assert data["status"] == "success"
        # Expect two calls, one for each ticker
        expected_calls = [call(123, "MSFT"), call(123, "AMZN")]
        assert mock_add.call_count == 2
        mock_add.assert_has_calls(expected_calls, any_order=False)
