from datetime import datetime, timezone
from unittest.mock import patch


def _login_row(
    *,
    user_id=1,
    email="user@example.com",
    username="user1",
    password_hash="$2b$12$fakehash",
    created_at=None,
    first_name="Ada",
    last_name="Lovelace",
    phone="+1 555 555 5555",
    country="US",
    timezone_name="America/Chicago",
    marketing_opt_in=True,
):
    return (
        user_id,
        email,
        username,
        password_hash,
        created_at or datetime.now(timezone.utc),
        first_name,
        last_name,
        phone,
        country,
        timezone_name,
        marketing_opt_in,
    )


def test_login_success_returns_expected_payload_without_extra_profile_fetch(client):
    with (
        patch("routes.user_routes.find_user_for_login", return_value=_login_row()) as mock_find,
        patch("routes.user_routes.verify_password", return_value=True) as mock_verify,
        patch("routes.user_routes._create_access_token", return_value="jwt-token"),
    ):
        response = client.post(
            "/api/login",
            json={"email_or_username": "user@example.com", "password": "correct-password"},
        )

    assert response.status_code == 200
    data = response.get_json()
    assert data["message"] == "Login successful"
    assert data["token"] == "jwt-token"
    assert data["user"] == {
        "id": 1,
        "email": "user@example.com",
        "username": "user1",
        "first_name": "Ada",
        "last_name": "Lovelace",
        "phone": "+1 555 555 5555",
        "country": "US",
        "timezone": "America/Chicago",
        "marketing_opt_in": True,
    }
    assert mock_find.call_count == 1
    assert mock_verify.call_count == 1


def test_login_invalid_credentials_when_user_missing(client):
    with patch("routes.user_routes.find_user_for_login", return_value=None):
        response = client.post(
            "/api/login",
            json={"email_or_username": "missing@example.com", "password": "password123"},
        )

    assert response.status_code == 401
    assert response.get_json()["error"] == "Invalid credentials"


def test_login_test_user_only_ensures_default_list(client):
    with (
        patch(
            "routes.user_routes.find_user_for_login",
            return_value=_login_row(email="test@gmail.com"),
        ),
        patch("routes.user_routes.verify_password", return_value=True),
        patch("routes.user_routes._create_access_token", return_value="jwt-token"),
        patch("routes.user_routes.create_empty_default_user_list") as mock_ensure_default,
        patch("routes.user_routes.create_default_user_list") as mock_seed_default,
    ):
        response = client.post(
            "/api/login",
            json={"email_or_username": "test@gmail.com", "password": "correct-password"},
        )

    assert response.status_code == 200
    mock_ensure_default.assert_called_once_with(1)
    mock_seed_default.assert_not_called()


def test_register_success_shape_is_unchanged(client):
    created_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    create_user_row = (
        7,
        "new@example.com",
        "new_user",
        created_at,
        "Grace",
        "Hopper",
        None,
        "US",
        "America/Chicago",
        False,
    )

    with (
        patch("routes.user_routes.create_user", return_value=create_user_row),
        patch("routes.user_routes.create_empty_default_user_list") as mock_empty_default,
        patch("routes.user_routes.create_default_user_list") as mock_seed_default,
        patch("routes.user_routes._create_access_token", return_value="reg-token"),
    ):
        response = client.post(
            "/api/register",
            json={
                "email": "new@example.com",
                "username": "new_user",
                "password": "password123",
                "first_name": "Grace",
                "last_name": "Hopper",
                "country": "US",
                "timezone": "America/Chicago",
                "marketing_opt_in": False,
                "honey_trap": "",
                "form_time": 8,
            },
        )

    assert response.status_code == 201
    data = response.get_json()
    assert data["message"] == "User registered successfully"
    assert data["token"] == "reg-token"
    assert data["user"] == {
        "id": 7,
        "email": "new@example.com",
        "username": "new_user",
        "first_name": "Grace",
        "last_name": "Hopper",
        "phone": None,
        "country": "US",
        "timezone": "America/Chicago",
        "marketing_opt_in": False,
        "created_at": created_at.isoformat(),
    }
    mock_empty_default.assert_called_once_with(7)
    mock_seed_default.assert_not_called()
