# test/test_user_repository.py
import pytest
from unittest.mock import patch
from datetime import datetime, timedelta
from database import user_repository

@pytest.fixture
def mock_conn():
    """
    Fixture to patch out the actual DB connection used by user_repository.
    """
    with patch("database.user_repository.get_connection") as mocked_get_connection:
        mocked_conn = mocked_get_connection.return_value
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
        (1, "testuser@example.com", "testuser", datetime.utcnow())     # find_user_by_email_or_username
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


