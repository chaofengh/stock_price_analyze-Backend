# tests/test_routes.py

import pytest
from unittest.mock import patch
from app import create_app
import tasks.daily_scan_tasks as daily_scan_tasks
from tasks.entry_decision_preload_tasks import (
    _reset_entry_decision_preload_state_for_tests,
    store_entry_decision_context,
    store_preloaded_entry_decision,
)

@pytest.fixture
def client():
    """
    A Pytest fixture that initializes a Flask test client.
    """
    # Pass testing=True so the CORS configuration is set appropriately
    app = create_app(testing=True)
    with app.test_client() as client:
        yield client

@patch('routes.summary_routes.get_summary')
def test_summary_endpoint(mock_summary, client):
    # Mock the result from get_summary()
    mock_summary.return_value = {'symbol': 'FAKE', 'analysis_period': 'test'}
    
    response = client.get('/api/summary?symbol=FAKE')
    assert response.status_code == 200
    data = response.get_json()
    assert data['symbol'] == 'FAKE'
    assert data['analysis_period'] == 'test'

def test_get_unknown_route(client):
    response = client.get('/api/not-a-real-route')
    assert response.status_code == 404


@patch('routes.summary_routes.get_summary_peers')
def test_summary_peers_returns_payload(mock_peers, client):
    mock_peers.return_value = {
        "symbol": "FAKE",
        "peer_info": {"AAA": {"latest_price": 101.5, "percentage_change": 1.2}},
    }
    response = client.get('/api/summary/peers?symbol=FAKE')
    assert response.status_code == 200
    data = response.get_json()
    assert data["symbol"] == "FAKE"
    assert data["peer_info"]["AAA"]["latest_price"] == 101.5
@patch('routes.summary_routes.get_summary_fundamentals')
def test_summary_fundamentals_returns_payload(mock_fundamentals, client):
    mock_fundamentals.return_value = {"symbol": "FAKE", "trailingPE": 12.5}
    response = client.get('/api/summary/fundamentals?symbol=FAKE')
    assert response.status_code == 200
    data = response.get_json()
    assert data["symbol"] == "FAKE"
    assert data["trailingPE"] == 12.5


@patch('routes.summary_routes.get_summary_peer_averages')
def test_summary_peer_averages_returns_payload(mock_peer_averages, client):
    mock_peer_averages.return_value = {"symbol": "FAKE", "avg_peer_trailingPE": 9.3}
    response = client.get('/api/summary/peer-averages?symbol=FAKE')
    assert response.status_code == 200
    data = response.get_json()
    assert data["symbol"] == "FAKE"
    assert data["avg_peer_trailingPE"] == 9.3


@patch('routes.summary_routes.request_full_entry_decision_preload')
def test_summary_entry_decision_starts_preload_when_result_is_missing(mock_request_preload, client):
    mock_request_preload.return_value = {"status": "started", "symbol": "FAKE"}

    response = client.get('/api/summary/entry-decision?symbol=FAKE&as_of_date=2026-04-15')
    assert response.status_code == 202

    data = response.get_json()
    assert data["status"] == "loading"
    assert data["symbol"] == "FAKE"
    assert data["requested_as_of_date"] == "2026-04-15"
    assert data["preload"]["status"] == "started"
    assert response.headers["Retry-After"] == "2"
    mock_request_preload.assert_called_once_with(
        "FAKE",
        as_of_date="2026-04-15",
        force=True,
        ignore_backoff=True,
    )


@patch('routes.summary_routes.request_full_entry_decision_preload')
def test_summary_entry_decision_returns_preloaded_payload(mock_request_preload, client):
    _reset_entry_decision_preload_state_for_tests()
    store_preloaded_entry_decision(
        "AMD",
        {
            "symbol": "AMD",
            "requested_as_of_date": "2026-04-15",
            "as_of_date": "2026-04-15",
            "setup_type": "lower_band_touch",
            "touched_side": "Lower",
            "horizons": {},
            "top_reasons": [],
            "backtest_1y": {},
            "chart_data": [],
        },
        as_of_date="2026-04-15",
    )

    response = client.get('/api/summary/entry-decision?symbol=amd&as_of_date=2026-04-15')

    assert response.status_code == 200
    data = response.get_json()
    assert data["symbol"] == "AMD"
    assert data["requested_as_of_date"] == "2026-04-15"
    assert data["setup_type"] == "lower_band_touch"
    mock_request_preload.assert_not_called()
    _reset_entry_decision_preload_state_for_tests()


@patch('routes.summary_routes.request_full_entry_decision_preload')
def test_summary_entry_decision_renders_selected_date_from_cached_model_context(mock_request_preload, client):
    _reset_entry_decision_preload_state_for_tests()
    context = {"symbol": "AMD", "feature_df": object(), "decisions_by_index": {}, "backtest_1y": {}}

    with (
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch(
            "tasks.entry_decision_preload_tasks.build_entry_decision_from_context",
            return_value={
                "symbol": "AMD",
                "requested_as_of_date": "2026-04-10",
                "as_of_date": "2026-04-10",
                "setup_type": "lower_band_touch",
                "touched_side": "Lower",
                "horizons": {},
                "top_reasons": [],
                "backtest_1y": {},
                "chart_data": [],
            },
        ) as mock_build_payload,
    ):
        assert store_entry_decision_context("AMD", context)
        response = client.get('/api/summary/entry-decision?symbol=AMD&as_of_date=2026-04-10')

    assert response.status_code == 200
    data = response.get_json()
    assert data["requested_as_of_date"] == "2026-04-10"
    mock_build_payload.assert_called_once_with(context, as_of_date="2026-04-10")
    mock_request_preload.assert_not_called()
    _reset_entry_decision_preload_state_for_tests()


@patch('routes.summary_routes.request_full_entry_decision_preload')
def test_summary_entry_decision_does_not_return_alert_snapshot_for_alert_ticker(mock_request_preload, client):
    _reset_entry_decision_preload_state_for_tests()
    daily_scan_tasks._reset_scan_state_for_tests()
    mock_request_preload.return_value = {"status": "started", "symbol": "AMD"}
    daily_scan_tasks._store_cached_result(
        {
            "timestamp": "2026-04-15 10:05:00",
            "alerts": [{"symbol": "AMD", "touched_side": "Upper"}],
        }
    )

    response = client.get('/api/summary/entry-decision?symbol=AMD&as_of_date=2026-04-15')

    assert response.status_code == 202
    data = response.get_json()
    assert data["status"] == "loading"
    assert data["symbol"] == "AMD"
    assert "horizons" not in data
    assert response.headers["Retry-After"] == "2"
    mock_request_preload.assert_called_once_with(
        "AMD",
        as_of_date="2026-04-15",
        force=True,
        ignore_backoff=True,
    )
    daily_scan_tasks._reset_scan_state_for_tests()
    _reset_entry_decision_preload_state_for_tests()


@patch('routes.summary_routes.request_full_entry_decision_preload')
def test_summary_entry_decision_keeps_loading_when_preload_is_in_symbol_backoff(mock_request_preload, client):
    mock_request_preload.return_value = {
        "status": "skipped",
        "reason": "interactive_symbol_preload_backoff",
        "symbol": "AMD",
        "retry_after_seconds": 15,
    }

    response = client.get('/api/summary/entry-decision?symbol=AMD&as_of_date=2026-04-15')

    assert response.status_code == 202
    data = response.get_json()
    assert data["status"] == "loading"
    assert data["preload"]["status"] == "loading"
    assert data["preload"]["reason"] == "interactive_symbol_preload_backoff"
    assert data["retry_after_seconds"] == 15
    assert response.headers["Retry-After"] == "15"


@patch('routes.summary_routes.request_full_entry_decision_preload')
def test_summary_entry_decision_keeps_loading_when_preload_worker_cannot_start(mock_request_preload, client):
    mock_request_preload.return_value = {
        "status": "error",
        "reason": "preload_start_failed",
        "symbol": "AMD",
        "retry_after_seconds": 30,
    }

    response = client.get('/api/summary/entry-decision?symbol=AMD&as_of_date=2026-04-15')

    assert response.status_code == 202
    data = response.get_json()
    assert data["status"] == "loading"
    assert data["preload"]["status"] == "loading"
    assert data["preload"]["reason"] == "preload_start_failed"
    assert response.headers["Retry-After"] == "30"


def test_summary_entry_decision_returns_400_for_validation_error(client):
    response = client.get('/api/summary/entry-decision?symbol=FAKE&as_of_date=x')
    assert response.status_code == 400
    data = response.get_json()
    assert data["error"] == "Invalid as_of_date 'x'. Expected YYYY-MM-DD."


def test_summary_entry_decision_returns_400_for_blank_symbol(client):
    response = client.get('/api/summary/entry-decision?symbol=')
    assert response.status_code == 400
    data = response.get_json()
    assert data["error"] == "symbol is required."


@patch('routes.summary_routes.request_full_entry_decision_preload')
def test_summary_entry_decision_treats_blank_as_of_date_as_latest(mock_request_preload, client):
    mock_request_preload.return_value = {"status": "started", "symbol": "FAKE", "retry_after_seconds": 3}

    response = client.get('/api/summary/entry-decision?symbol=FAKE&as_of_date=')

    assert response.status_code == 202
    data = response.get_json()
    assert data["requested_as_of_date"] is None
    assert response.headers["Retry-After"] == "3"
    mock_request_preload.assert_called_once_with(
        "FAKE",
        as_of_date=None,
        force=True,
        ignore_backoff=True,
    )
