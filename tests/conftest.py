# tests/conftest.py
import os
from dotenv import load_dotenv
from unittest.mock import patch, MagicMock

# Load environment variables from .env (if exists)
load_dotenv()

# Override DATABASE_URL to avoid accidental real DB connections
os.environ["DATABASE_URL"] = "postgresql://dummy:dummy@localhost/dummy"

# IMPORTANT: Patch the database connection before any imports that might trigger it.
connection_patch = patch('database.connection.get_connection', return_value=MagicMock())
connection_patch.start()

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

    yield flask_app

    # Stop patching after tests are complete
    connection_patch.stop()

@pytest.fixture
def client(app):
    """
    Returns a Flask test client for making requests to the app.
    """
    with app.test_client() as client:
        yield client
