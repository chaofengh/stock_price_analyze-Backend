# tests/test_routes.py

import pytest
from unittest.mock import patch
from app import create_app

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


@patch('routes.summary_routes.get_entry_decision')
def test_summary_entry_decision_returns_payload(mock_entry_decision, client):
    mock_entry_decision.return_value = {
        "symbol": "FAKE",
        "requested_as_of_date": "2026-04-15",
        "as_of_date": "2026-04-15",
        "date_was_snapped": False,
        "setup_type": "upper_band_touch",
        "touched_side": "Upper",
        "prediction_threshold": 0.75,
        "horizons": {
            "5d": {
                "status": "prediction",
                "predicted_direction": "continuation",
                "continuation_probability": 0.64,
                "reversal_probability": 0.36,
                "confidence_score": 64,
                "no_prediction_reason": None,
            },
            "10d": {
                "status": "no_prediction",
                "predicted_direction": None,
                "continuation_probability": 0.52,
                "reversal_probability": 0.48,
                "confidence_score": 52,
                "no_prediction_reason": "low_confidence",
            },
        },
        "top_reasons": [],
        "backtest_1y": {"5d": {"prediction_count": 12, "accuracy": 0.5}},
        "chart_data": [],
    }

    response = client.get('/api/summary/entry-decision?symbol=FAKE&as_of_date=2026-04-15')
    assert response.status_code == 200

    data = response.get_json()
    assert data["symbol"] == "FAKE"
    assert data["setup_type"] == "upper_band_touch"
    assert data["horizons"]["5d"]["predicted_direction"] == "continuation"
    assert data["horizons"]["10d"]["no_prediction_reason"] == "low_confidence"
    assert data["backtest_1y"]["5d"]["prediction_count"] == 12
    mock_entry_decision.assert_called_once_with("FAKE", as_of_date="2026-04-15")


@patch('routes.summary_routes.get_entry_decision')
def test_summary_entry_decision_returns_400_for_validation_error(mock_entry_decision, client):
    mock_entry_decision.side_effect = ValueError("Invalid as_of_date 'x'. Expected YYYY-MM-DD.")

    response = client.get('/api/summary/entry-decision?symbol=FAKE&as_of_date=x')
    assert response.status_code == 400
    data = response.get_json()
    assert data["error"] == "Invalid as_of_date 'x'. Expected YYYY-MM-DD."
