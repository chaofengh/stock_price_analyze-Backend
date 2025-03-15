# tests/conftest.py
import pytest
from app import create_app
import os
os.environ["DATABASE_URL"] = "postgresql://dummy:dummy@localhost/dummy"


@pytest.fixture(scope="session")
def app():
    """
    Creates and configures a new app instance for tests.
    The scope is 'session' so we reuse the same app object for all tests.
    """
    flask_app = create_app()
    flask_app.testing = True
    return flask_app

@pytest.fixture
def client(app):
    """
    Returns a Flask test client for making requests to the app.
    """
    with app.test_client() as client:
        yield client
