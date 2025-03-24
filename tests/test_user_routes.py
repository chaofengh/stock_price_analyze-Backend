# tests/test_user_routes.py
import pytest
from app import create_app

@pytest.fixture
def client():
    app = create_app(testing=True)
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client

@pytest.fixture(autouse=True)
def patch_captcha(monkeypatch):
    # Patch verify_captcha in the user_routes module to always return True.
    from routes import user_routes
    monkeypatch.setattr(user_routes, "verify_captcha", lambda token: True)

@pytest.fixture(autouse=True)
def patch_send_email(monkeypatch):
    # Patch send_reset_email so no actual email is sent.
    from routes import user_routes
    monkeypatch.setattr(user_routes, "send_reset_email", lambda to_email, reset_token: None)

def test_register(client):
    payload = {
        "email": "newuser@example.com",
        "username": "newuser",
        "password": "NewUserPassword123",
        "captcha_token": "dummy"
    }
    response = client.post("/api/register", json=payload)
    assert response.status_code == 201
    data = response.get_json()
    assert "user" in data
    assert data["user"]["email"] == payload["email"]

def test_register_invalid_email(client):
    payload = {
        "email": "invalidemail",
        "username": "userinvalid",
        "password": "NewUserPassword123",
        "captcha_token": "dummy"
    }
    response = client.post("/api/register", json=payload)
    assert response.status_code == 400

def test_login(client):
    # First, register a user.
    reg_payload = {
        "email": "loginuser@example.com",
        "username": "loginuser",
        "password": "LoginPassword123",
        "captcha_token": "dummy"
    }
    reg_resp = client.post("/api/register", json=reg_payload)
    assert reg_resp.status_code == 201

    # Now test login.
    login_payload = {
        "email_or_username": "loginuser@example.com",
        "password": "LoginPassword123",
        "captcha_token": "dummy"
    }
    login_resp = client.post("/api/login", json=login_payload)
    assert login_resp.status_code == 200
    data = login_resp.get_json()
    assert "token" in data

def test_forgot_password(client):
    # First, register a user.
    reg_payload = {
        "email": "forgotuser@example.com",
        "username": "forgotuser",
        "password": "ForgotPassword123",
        "captcha_token": "dummy"
    }
    reg_resp = client.post("/api/register", json=reg_payload)
    assert reg_resp.status_code == 201

    # Test forgot_password.
    forgot_payload = {
        "email": "forgotuser@example.com",
        "captcha_token": "dummy"
    }
    forgot_resp = client.post("/api/forgot_password", json=forgot_payload)
    assert forgot_resp.status_code == 200
