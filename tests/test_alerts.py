# tests/test_alerts.py
import json
from unittest.mock import patch

def test_get_latest_alerts_no_data(client):
    """
    Test the /api/alerts/latest endpoint when there is no scan result.
    """
    # Mock the tasks.daily_scan_tasks.latest_scan_result to be None or empty
    with patch("routes.alerts_routes.latest_scan_result", None):
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
    with patch("routes.alerts_routes.latest_scan_result", fake_scan_result):
        response = client.get("/api/alerts/latest")
        assert response.status_code == 200
        data = response.get_json()
        assert data == fake_scan_result

def test_alerts_stream(client):
    """
    Test the /api/alerts/stream SSE endpoint.
    SSE is tricky to fully test in a synchronous manner, but we can at least
    check we get a 200 response and the correct MIME type.
    """
    with patch("routes.alerts_routes.latest_scan_result", {
        "timestamp": "2023-01-01 10:00:00",
        "alerts": []
    }):
        response = client.get("/api/alerts/stream")
        # SSE responses should return a 200 and the `text/event-stream` MIME type
        assert response.status_code == 200
        assert response.mimetype == "text/event-stream"
        # You could also attempt reading the generator, but that requires more advanced SSE mocking.
