# tests/test_summary.py
from unittest.mock import patch

def test_summary_endpoint_success(client):
    fake_summary = {
        "symbol": "QQQ",
        "analysis_period": "2023-01-01 to 2023-03-01",
        "trading_days": 40,
        # etc... fill in minimal/representative fields
    }
    with patch("routes.summary_routes.get_summary", return_value=fake_summary):
        response = client.get("/api/summary?symbol=QQQ")
        assert response.status_code == 200
        data = response.get_json()
        assert data == fake_summary
