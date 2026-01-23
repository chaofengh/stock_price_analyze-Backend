#conftest.py
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import pytest
import psycopg2

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app import create_app
from database.connection import get_connection
from database.create_user_table import create_users_table
from database.create_ticker_table import create_tickers_table
from database.create_lists_table import create_lists_and_list_tickers_tables

# Load environment variables from .env (if exists)
load_dotenv()

def _resolve_db_url():
    return (
        os.getenv("external_database_url")
    )


def _ensure_python_312():
    if sys.version_info < (3, 12):
        raise RuntimeError("Tests must be run with Python 3.12+.")

@pytest.fixture(scope="session")
def db_url():
    """
    Provide the database URL for integration tests.
    Prefer TEST_DATABASE_URL when set.
    """
    _ensure_python_312()
    db_url = _resolve_db_url()
    if not db_url:
        pytest.skip("DATABASE_URL or TEST_DATABASE_URL must be set to run DB integration tests.")
    os.environ["DATABASE_URL"] = db_url
    return db_url

@pytest.fixture(scope="session")
def db_setup(db_url):
    """
    Ensure required tables exist for DB integration tests.
    """
    conn = get_connection()
    conn.close()
    create_users_table()
    create_tickers_table()
    create_lists_and_list_tickers_tables()

@pytest.fixture
def db_connection(db_setup):
    """
    Real database connection for integration tests.
    """
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()

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

@pytest.fixture
def client(app):
    """
    Returns a Flask test client for making requests to the app.
    This client fixture is available to all test modules.
    """
    with app.test_client() as client:
        yield client
