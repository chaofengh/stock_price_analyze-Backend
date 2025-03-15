# tests/conftest.py
import os
from dotenv import load_dotenv
from unittest.mock import patch, MagicMock

# Load environment variables from .env first (if exists)
load_dotenv()

# Override DATABASE_URL to avoid accidental connections
os.environ["DATABASE_URL"] = "postgresql://dummy:dummy@localhost/dummy"

import pytest
from app import create_app

@pytest.fixture(scope="session")
def app():
    """
    Creates and configures a new app instance for tests.
    The scope is 'session' so we reuse the same app object for all tests.
    """
    flask_app = create_app(testing=True)
    flask_app.testing = True

    # Patch the get_connection function globally for the duration of tests
    connection_patch = patch('database.connection.get_connection')
    mocked_connection = connection_patch.start()

    # Configure mocked_connection to return a MagicMock object
    mocked_connection.return_value = MagicMock()

    # Teardown: stop patching when tests are done
    yield flask_app
    connection_patch.stop()


@pytest.fixture
def client(app):
    """
    Returns a Flask test client for making requests to the app.
    """
    with app.test_client() as client:
        yield client
