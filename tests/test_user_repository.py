# tests/test_user_repository.py
from datetime import datetime
import pytest
from database import user_repository

def test_hash_and_verify_password():
    plain_password = "TestPassword123!"
    hashed = user_repository.hash_password(plain_password)
    assert hashed != plain_password
    assert user_repository.verify_password(plain_password, hashed)
    assert not user_repository.verify_password("WrongPassword", hashed)

def test_create_and_find_user():
    email = "testuser@example.com"
    username = "testuser"
    password = "TestPassword123!"
    
    user = user_repository.create_user(email, username, password)
    assert user is not None
    # user tuple: (id, email, username, created_at)
    assert user[1] == email
    assert user[2] == username

    found = user_repository.find_user_by_email_or_username(email)
    assert found is not None
    assert found[1] == email
    assert found[2] == username

def test_set_reset_token():
    email = "resetuser@example.com"
    username = "resetuser"
    password = "ResetPassword123!"
    user = user_repository.create_user(email, username, password)
    user_id = user[0]
    
    token = user_repository.set_reset_token(user_id)
    assert token is not None
    
    found = user_repository.find_user_by_email(email)
    assert found is not None
    # found[4] is reset_token and found[5] is reset_token_expires
    assert found[4] == token
    assert isinstance(found[5], datetime)
    assert found[5] > datetime.utcnow()
