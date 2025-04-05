import json
from datetime import datetime, timedelta
from unittest.mock import patch
import pytest

# Import the function under test.
from routes.user_routes import set_reset_token


def test_alerts_stream(client):
    """
    Test the /api/alerts/stream SSE endpoint.
    """
    with patch("routes.alerts_routes.daily_scan_tasks.latest_scan_result", {
        "timestamp": "2023-01-01 10:00:00",
        "alerts": []
    }):
        response = client.get("/api/alerts/stream")
        # SSE responses should return a 200 and the `text/event-stream` MIME type.
        assert response.status_code == 200
        assert response.mimetype == "text/event-stream"


