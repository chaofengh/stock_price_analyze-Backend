import json
from datetime import datetime, timedelta
from unittest.mock import patch
import pytest

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

def test_set_reset_token(mock_conn):
    """
    Test that setting a reset token works with a mocked DB.
    """
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value

    # We'll have 3 calls to fetchone():
    # 1) for create_user
    # 2) for set_reset_token (if it does a SELECT after the update or uses RETURNING)
    # 3) for find_user_by_email
    mock_cursor.fetchone.side_effect = [
        # create_user result
        (123, "resetuser@example.com", "resetuser", datetime.utcnow()),
        # set_reset_token result
        ("RandomGeneratedToken123",),
        # find_user_by_email result with a valid reset token expiration using timedelta
        (123, "resetuser@example.com", "resetuser", "some_hashed_password", "RandomGeneratedToken123", datetime.utcnow() + timedelta(seconds=3600))
    ]
    
    # Further testing logic for set_reset_token should follow here.
