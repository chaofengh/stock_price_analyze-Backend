import json
from unittest.mock import patch
from flask import Flask
import pytest

# Assuming your Flask app + alerts_blueprint is defined somewhere else, e.g. "app.py" or "routes/alerts_routes.py".
# and you have a "client" fixture that provides a Flask test client.
# These imports may vary depending on how your project is structured.
# Example:
# from routes.alerts_routes import alerts_blueprint
# from app import create_app

@pytest.fixture
def client():
    # Example fixture to create a test client – adapt to your setup
    app = Flask(__name__)
    # app.register_blueprint(alerts_blueprint)  # If needed
    app.testing = True
    with app.test_client() as test_client:
        yield test_client

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
        # Optionally, you could try reading the streamed data, but
        # SSE is trickier to test fully in a synchronous environment.
