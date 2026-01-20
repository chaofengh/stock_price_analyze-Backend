import json
from datetime import datetime, timedelta
from unittest.mock import patch
import pytest

# Import the function under test.
def test_alerts_stream(client):
    """
    Test the /api/alerts/stream SSE endpoint.
    """
    with (
        patch(
            "routes.alerts_routes.get_latest_scan_result",
            return_value={"timestamp": "2023-01-01 10:00:00", "alerts": []},
        ),
        patch("routes.alerts_routes.get_all_tickers", return_value=[]),
        patch("routes.alerts_routes.authenticate_bearer_token") as mock_auth,
    ):
        mock_auth.return_value = type("Auth", (), {"user_id": 1})()
        response = client.get("/api/alerts/stream", headers={"Authorization": "Bearer test"})

    # SSE responses should return a 200 and the `text/event-stream` MIME type.
    assert response.status_code == 200
    assert response.mimetype == "text/event-stream"


def test_alert_filter_uses_symbol_key():
    result = {
        "timestamp": "2023-01-01 10:00:00",
        "alerts": [
            {"symbol": "META"},
            {"symbol": "aapl"},
            {"ticker": "tsla"},
            {"symbol": None},
        ],
    }
    with patch("routes.alerts_routes.get_all_tickers", return_value=["meta", "TSLA"]):
        from routes.alerts_routes import _filter_for_user

        filtered = _filter_for_user(result, user_id=1)

    assert [a.get("symbol") or a.get("ticker") for a in filtered["alerts"]] == ["META", "tsla"]
