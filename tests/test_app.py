# tests/test_app.py

import pytest
from app import create_app

@pytest.fixture
def client():
    """
    A Pytest fixture that initializes a Flask test client.
    """
    app = create_app()
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

def test_create_app():
    """
    Basic test to ensure the Flask app is created with no errors.
    """
    app = create_app()
    assert app is not None
    assert app.name == 'app'

def test_api_root_not_found(client):
    """
    Example test to confirm that hitting an unknown route returns a 404.
    """
    response = client.get('/api/unknown-route')
    assert response.status_code == 404
