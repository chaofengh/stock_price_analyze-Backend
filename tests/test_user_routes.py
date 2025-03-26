# tests/test_user_routes.py
import pytest
from datetime import datetime, timedelta
import datetime as dt

from app import create_app

# --- Fake implementations for database operations ---

def fake_create_user(email, username, password):
    """
    Simulate creating a user in the database.
    Returns a tuple: (id, email, username, created_at)
    """
    return (123, email, username, dt.datetime.utcnow())

def fake_find_user_by_email_or_username(email_or_username):
    """
    Simulate looking up a user by email or username.
    For the login test, if the provided value matches the registered email/username,
    return a user tuple with the known password.
    """
    if email_or_username in ["loginuser@example.com", "loginuser"]:
        # Tuple: (id, email, username, password_hash, reset_token, reset_token_expires)
        return (123, "loginuser@example.com", "loginuser", "LoginPassword123", None, None)
    return None

def fake_find_user_by_email(email):
    """
    Simulate finding a user by email.
    For registration and forgot password, return a valid tuple if the email matches.
    """
    if email in ["forgotuser@example.com", "newuser@example.com"]:
        # For registration, reset token info may be None.
        return (123, email, "dummy", "SomeHashedPassword", None, None)
    return None

def fake_set_reset_token(user_id):
    """
    Simulate setting a reset token.
    Always returns a fixed token.
    """
    return "FixedResetToken"

def fake_verify_password(plain, hashed):
    """
    For login purposes, simply check that the plain password equals the expected value.
    (This works because in fake_create_user we don’t actually hash the password.)
    """
    # For our login test, we expect the password to be "LoginPassword123"
    return plain == "LoginPassword123"

# --- Patching the database functions ---
@pytest.fixture(autouse=True)
def patch_user_repository(monkeypatch):
    from database import user_repository
    monkeypatch.setattr(user_repository, "create_user", fake_create_user)
    monkeypatch.setattr(user_repository, "find_user_by_email_or_username", fake_find_user_by_email_or_username)
    monkeypatch.setattr(user_repository, "find_user_by_email", fake_find_user_by_email)
    monkeypatch.setattr(user_repository, "set_reset_token", fake_set_reset_token)
    monkeypatch.setattr(user_repository, "verify_password", fake_verify_password)

# --- App Client Fixture ---
@pytest.fixture
def client():
    app = create_app(testing=True)
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client

# --- Patch send_reset_email so no real email is sent ---
@pytest.fixture(autouse=True)
def patch_send_email(monkeypatch):
    from routes import user_routes
    monkeypatch.setattr(user_routes, "send_reset_email", lambda to_email, reset_token: None)

# --- Tests for endpoints ---

def test_register(client):
    payload = {
        "email": "newuser@example.com",
        "username": "newuser",
        "password": "NewUserPassword123",
        "honey_trap": "",
        "form_time": 10  # form submitted after 10 seconds
    }
    response = client.post("/api/register", json=payload)
    # Expecting a 201 CREATED status code.
    assert response.status_code == 201
    data = response.get_json()
    assert "user" in data
    # The returned user email should match the payload.
    assert data["user"]["email"] == payload["email"]

def test_register_invalid_email(client):
    payload = {
        "email": "invalidemail",
        "username": "userinvalid",
        "password": "NewUserPassword123",
        "honey_trap": "",
        "form_time": 10
    }
    response = client.post("/api/register", json=payload)
    # Should return a 400 for invalid email format.
    assert response.status_code == 400

def test_login(client):
    # First, register a user.
    reg_payload = {
        "email": "loginuser@example.com",
        "username": "loginuser",
        "password": "LoginPassword123",
        "honey_trap": "",
        "form_time": 10
    }
    reg_resp = client.post("/api/register", json=reg_payload)
    assert reg_resp.status_code == 201

    # Now test login.
    login_payload = {
        "email_or_username": "loginuser@example.com",
        "password": "LoginPassword123",
        "honey_trap": "",
        "form_time": 10
    }
    login_resp = client.post("/api/login", json=login_payload)
    assert login_resp.status_code == 200
    data = login_resp.get_json()
    # Expect a token to be returned on successful login.
    assert "token" in data

def test_forgot_password(client):
    # First, register a user.
    reg_payload = {
        "email": "forgotuser@example.com",
        "username": "forgotuser",
        "password": "ForgotPassword123",
        "honey_trap": "",
        "form_time": 10
    }
    reg_resp = client.post("/api/register", json=reg_payload)
    assert reg_resp.status_code == 201

    # Test forgot_password endpoint.
    forgot_payload = {
        "email": "forgotuser@example.com",
        "honey_trap": "",
        "form_time": 10
    }
    forgot_resp = client.post("/api/forgot_password", json=forgot_payload)
    # Expect a 200 status code and a message indicating a reset link was sent.
    assert forgot_resp.status_code == 200
