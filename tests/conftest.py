# tests/conftest.py
import os
from dotenv import load_dotenv
from unittest.mock import patch, MagicMock
import pytest

# Load environment variables from .env (if exists)
load_dotenv()

# Override DATABASE_URL to avoid accidental real DB connections
os.environ["DATABASE_URL"] = "postgresql://dummy:dummy@localhost/dummy"

# IMPORTANT: Patch the database connection before any imports that might trigger it.
connection_patch = patch('database.connection.get_connection', return_value=MagicMock())
connection_patch.start()

from app import create_app

@pytest.fixture(scope="session")
def app():
    """
    Creates and configures a new Flask app instance for tests.
    The app is created in testing mode, ensuring that all blueprints (including alerts, user, etc.)
    are registered. The fixture scope is 'session' so that the same app object is reused across tests.
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
    This client fixture is available to all test modules.
    """
    with app.test_client() as client:
        yield client
