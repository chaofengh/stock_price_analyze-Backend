from unittest.mock import patch

from tasks.entry_decision_preload_tasks import _reset_entry_decision_preload_state_for_tests

# Import the function under test.
def test_alerts_latest_returns_payload_shape(client):
    payload = {
        "timestamp": "2026-04-06 10:35:00",
        "alerts": [{"symbol": "AAPL"}],
        "meta": {"next_run_at": "2026-04-06 11:35:00", "is_official": True},
    }
    with (
        patch("routes.alerts_routes.get_latest_scan_result", return_value=payload),
        patch("routes.alerts_routes.get_all_tickers", return_value=["AAPL"]),
        patch("routes.alerts_routes.preload_entry_decisions_from_alert_payload") as mock_preload,
        patch("routes.alerts_routes.authenticate_bearer_token") as mock_auth,
    ):
        mock_auth.return_value = type("Auth", (), {"user_id": 1})()
        response = client.get("/api/alerts/latest", headers={"Authorization": "Bearer test"})

    assert response.status_code == 200
    data = response.get_json()
    assert data["timestamp"] == payload["timestamp"]
    assert data["meta"]["next_run_at"] == payload["meta"]["next_run_at"]
    assert data["meta"]["is_official"] is True
    assert data["alerts"] == [{"symbol": "AAPL"}]
    mock_preload.assert_called_once_with(data)


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
        patch("routes.alerts_routes.preload_entry_decisions_from_alert_payload"),
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


def test_alerts_latest_preloads_visible_ticker_and_entry_endpoint_returns_cached_result(client):
    _reset_entry_decision_preload_state_for_tests()
    payload = {
        "timestamp": "2026-04-15 10:35:00",
        "alerts": [{"symbol": "UBER"}, {"symbol": "AMD"}],
        "meta": {"next_run_at": "2026-04-15 11:05:00", "is_official": True},
    }

    def _entry_decision(symbol, as_of_date=None):
        return {
            "symbol": symbol,
            "requested_as_of_date": as_of_date,
            "as_of_date": as_of_date,
            "horizons": {"5d": {"status": "prediction"}},
            "top_reasons": [],
            "backtest_1y": {},
            "chart_data": [],
        }

    try:
        with (
            patch.dict(
                "os.environ",
                {"ENTRY_DECISION_PRELOAD_FULL_ENABLED": "1", "ENTRY_DECISION_PRELOAD_INLINE": "1"},
            ),
            patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
            patch("routes.summary_routes.refresh_entry_decision_preload_state", return_value=None),
            patch("routes.summary_routes.request_full_entry_decision_preload") as mock_request_preload,
            patch("tasks.entry_decision_preload_tasks.get_entry_decision", side_effect=_entry_decision) as mock_entry,
            patch("routes.alerts_routes.get_latest_scan_result", return_value=payload),
            patch("routes.alerts_routes.get_all_tickers", return_value=["UBER", "AMD"]),
            patch("routes.alerts_routes.authenticate_bearer_token") as mock_auth,
        ):
            mock_auth.return_value = type("Auth", (), {"user_id": 1})()
            alerts_response = client.get("/api/alerts/latest", headers={"Authorization": "Bearer test"})
            entry_response = client.get(
                "/api/summary/entry-decision?symbol=UBER&as_of_date=2026-04-15"
            )

        assert alerts_response.status_code == 200
        assert entry_response.status_code == 200
        assert entry_response.get_json()["symbol"] == "UBER"
        mock_request_preload.assert_not_called()
        assert [call.args[0] for call in mock_entry.call_args_list] == ["UBER", "AMD"]
        assert [call.kwargs["as_of_date"] for call in mock_entry.call_args_list] == [
            "2026-04-15",
            "2026-04-15",
        ]
    finally:
        _reset_entry_decision_preload_state_for_tests()
