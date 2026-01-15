import uuid
from database import user_repository

def test_hash_and_verify_password():
    plain_password = "TestPassword123!"
    hashed = user_repository.hash_password(plain_password)
    assert hashed != plain_password
    assert user_repository.verify_password(plain_password, hashed)
    assert not user_repository.verify_password("WrongPassword", hashed)

def test_create_and_find_user(db_connection):
    """
    Test user creation and then finding by email/username using a real DB connection.
    """
    suffix = uuid.uuid4().hex[:8]
    email = f"testuser_{suffix}@example.com"
    username = f"testuser_{suffix}"
    password = "TestPassword123!"

    user_id = None
    try:
        user = user_repository.create_user(email, username, password)
        assert user is not None
        # user is a tuple: (id, email, username, created_at)
        user_id = user[0]
        assert user[1] == email
        assert user[2] == username

        found = user_repository.find_user_by_email_or_username(email)
        assert found is not None
        assert found[1] == email
        assert found[2] == username
    finally:
        if user_id is not None:
            with db_connection.cursor() as cur:
                cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
            db_connection.commit()
