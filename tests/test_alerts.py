import json
from datetime import datetime, timedelta
from unittest.mock import patch
import pytest

# Import the function under test.
# Adjust the import path as needed for your project.
from routes.user_routes import set_reset_token

def test_get_latest_alerts_no_data(client):
    """
    Test the /api/alerts/latest endpoint when there is no scan result.
    """
    # Patch where 'latest_scan_result' actually lives
    with patch("routes.alerts_routes.daily_scan_tasks.latest_scan_result", None):
        response = client.get("/api/alerts/latest")
        assert response.status_code == 200
        data = response.get_json()
        assert data["timestamp"] is None
        assert data["alerts"] == []

def test_get_latest_alerts_with_data(client):
    """
    Test the /api/alerts/latest endpoint when there's a scan result.
    """
    fake_scan_result = {
        "timestamp": "2023-01-01 10:00:00",
        "alerts": [
            {"symbol": "TSLA", "message": "Price spike detected"},
            {"symbol": "AAPL", "message": "Unusual volume"}
        ]
    }
    with patch("routes.alerts_routes.daily_scan_tasks.latest_scan_result", fake_scan_result):
        response = client.get("/api/alerts/latest")
        assert response.status_code == 200
        data = response.get_json()
        assert data == fake_scan_result

def test_alerts_stream(client):
    """
    Test the /api/alerts/stream SSE endpoint.
    """
    with patch("routes.alerts_routes.daily_scan_tasks.latest_scan_result", {
        "timestamp": "2023-01-01 10:00:00",
        "alerts": []
    }):
        response = client.get("/api/alerts/stream")
        # SSE responses should return a 200 and the `text/event-stream` MIME type
        assert response.status_code == 200
        assert response.mimetype == "text/event-stream"

# Define a fixture to retrieve the mocked DB connection.
@pytest.fixture
def mock_conn():
    from database.connection import get_connection
    return get_connection()

def test_set_reset_token(mock_conn):
    """
    Test that setting a reset token works with a mocked DB.
    """
    # Access the mocked cursor from the connection.
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value

    # Set up the side effects for fetchone():
    # 1) For create_user (user exists)
    # 2) For set_reset_token (token generation, e.g., via RETURNING clause)
    # 3) For find_user_by_email (to verify the new token and expiration)
    mock_cursor.fetchone.side_effect = [
        # Simulated create_user result
        (123, "resetuser@example.com", "resetuser", datetime.utcnow()),
        # Simulated result of setting the reset token
        ("RandomGeneratedToken123",),
        # Simulated find_user_by_email result with token and expiration updated
        (123, "resetuser@example.com", "resetuser", "some_hashed_password", "RandomGeneratedToken123", datetime.utcnow() + timedelta(seconds=3600))
    ]
    
    # Call the function under test which should use the mocked DB connection.
    token = set_reset_token("resetuser@example.com")
    assert token == "RandomGeneratedToken123"
