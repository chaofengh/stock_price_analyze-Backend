# tests/conftest.py
import os
from dotenv import load_dotenv
from unittest.mock import patch, MagicMock
import pytest
from datetime import datetime, timedelta

# Load environment variables from .env (if exists)
load_dotenv()

# Override DATABASE_URL to avoid accidental real DB connections
os.environ["DATABASE_URL"] = "postgresql://dummy:dummy@localhost/dummy"

# Define a fake get_connection function that returns a fake connection
def fake_get_connection():
    connection = MagicMock()
    fake_cursor = MagicMock()
    connection.cursor.return_value.__enter__.return_value = fake_cursor
    # Default behavior for create_user: return a dummy user row
    fake_cursor.fetchone.return_value = (1, "dummy@example.com", "dummy", datetime.utcnow())
    return connection

# Patch the database connection before any imports that might trigger it.
connection_patch = patch('database.connection.get_connection', side_effect=fake_get_connection)
connection_patch.start()

from app import create_app

@pytest.fixture(scope="session")
def app():
    """
    Creates and configures a new Flask app instance for tests.
    The app is created in testing mode, ensuring that all blueprints (including alerts, user, etc.)
    are registered. For alerts, we explicitly set a URL prefix to match test expectations.
    """
    flask_app = create_app(testing=True)
    flask_app.testing = True

    # Re-register alerts_blueprint with a url_prefix to match test endpoints.
    # This ensures that endpoints such as /api/alerts/latest and /api/alerts/stream are available.
    from routes.alerts_routes import alerts_blueprint
    flask_app.register_blueprint(alerts_blueprint, url_prefix="/api/alerts")
    
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
