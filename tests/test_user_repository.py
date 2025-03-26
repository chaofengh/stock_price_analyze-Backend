import pytest
from unittest.mock import patch
from datetime import datetime
from database import user_repository

@pytest.fixture
def mock_conn():
    """
    Fixture to patch out the actual DB connection used by user_repository.
    """
    with patch("database.user_repository.conn") as mocked_conn:
        mock_cursor = mocked_conn.cursor.return_value.__enter__.return_value
        # You can set up default side effects if needed across multiple tests:
        # mock_cursor.fetchone.side_effect = [...]
        yield mocked_conn

def test_hash_and_verify_password():
    plain_password = "TestPassword123!"
    hashed = user_repository.hash_password(plain_password)
    assert hashed != plain_password
    assert user_repository.verify_password(plain_password, hashed)
    assert not user_repository.verify_password("WrongPassword", hashed)

def test_create_and_find_user(mock_conn):
    """
    Test user creation and then finding by email/username, 
    using mock_conn so no real DB is involved.
    """
    # Setup the rows that should come back from fetchone() calls:
    # 1) after create_user does an INSERT + SELECT
    # 2) after find_user_by_email_or_username
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
    mock_cursor.fetchone.side_effect = [
        (1, "testuser@example.com", "testuser", datetime.utcnow()),  # create_user result
        (1, "testuser@example.com", "testuser", datetime.utcnow())   # find_user_by_email_or_username
    ]

    email = "testuser@example.com"
    username = "testuser"
    password = "TestPassword123!"

    user = user_repository.create_user(email, username, password)
    assert user is not None
    # user is a tuple: (id, email, username, created_at)
    assert user[1] == email
    assert user[2] == username

    found = user_repository.find_user_by_email_or_username(email)
    assert found is not None
    assert found[1] == email
    assert found[2] == username

def test_set_reset_token(mock_conn):
    """
    Test that setting a reset token works with a mocked DB.
    """
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value

    # We'll have 3 calls to fetchone():
    # 1) for create_user
    # 2) for set_reset_token (if it does a SELECT after the update or uses RETURNING)
    # 3) for find_user_by_email
    mock_cursor.fetchone.side_effect = [
        # create_user result
        (123, "resetuser@example.com", "resetuser", datetime.utcnow()), 
        # set_reset_token might do an UPDATE with a RETURNING token or a SELECT, 
        # if you code it that way. If your code only returns the token as a new random string
        # in Python, you might not rely on fetchone here; adjust as needed.
        ("RandomGeneratedToken123",), 
        # find_user_by_email => returns (id, email, username, password, reset_token, reset_token_expires)
        (123, "resetuser@example.com", "resetuser", "some_hashed_password", "RandomGeneratedToken123", datetime.utcnow() + 3600)
    ]

    email = "resetuser@example.com"
    username = "resetuser"
    password = "ResetPassword123!"
    user = user_repository.create_user(email, username, password)
    user_id = user[0]

    token = user_repository.set_reset_token(user_id)
    assert token is not None
    assert token == "RandomGeneratedToken123"

    found = user_repository.find_user_by_email(email)
    assert found is not None
    # Suppose found[4] = reset_token, found[5] = reset_token_expires
    assert found[4] == "RandomGeneratedToken123"
    assert isinstance(found[5], datetime)
    assert found[5] > datetime.utcnow()
