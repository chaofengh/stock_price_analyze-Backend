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


@patch('routes.summary_routes.get_summary_fundamentals')
def test_summary_fundamentals_pending_when_empty(mock_fundamentals, client):
    mock_fundamentals.return_value = {}
    response = client.get('/api/summary/fundamentals?symbol=FAKE')
    assert response.status_code == 200
    data = response.get_json()
    assert data['status'] == 'pending'


@patch('routes.summary_routes.get_summary_peer_averages')
@patch('routes.summary_routes.get_summary_fundamentals')
def test_summary_peer_averages_pending_without_fundamentals(
    mock_fundamentals,
    mock_peer_averages,
    client,
):
    mock_fundamentals.return_value = {}
    response = client.get('/api/summary/peer-averages?symbol=FAKE')
    assert response.status_code == 200
    data = response.get_json()
    assert data['status'] == 'pending'
    mock_peer_averages.assert_not_called()
