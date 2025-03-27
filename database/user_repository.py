import secrets
from datetime import datetime, timezone, timedelta
from passlib.context import CryptContext
from .connection import get_connection

# Configure Passlib to use bcrypt which automatically salts passwords.
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def hash_password(plain_password: str) -> str:
    """
    Hash the plain-text password using bcrypt.
    A unique salt is generated automatically.
    """
    return pwd_context.hash(plain_password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verify a plain-text password against the hashed (and salted) password.
    """
    return pwd_context.verify(plain_password, hashed_password)

def create_user(email: str, username: str, password: str):
    """
    Inserts a new user into the database with a hashed (and salted) password.
    Returns the newly created user row containing (id, email, username, created_at).
    """
    hashed = hash_password(password)
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (email, username, password_hash)
                VALUES (%s, %s, %s)
                RETURNING id, email, username, created_at;
            """, (email, username, hashed))
            user_row = cur.fetchone()
        conn.commit()
        return user_row
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def find_user_by_email_or_username(email_or_username: str):
    """
    Retrieve a user row by email or username.
    Returns a tuple:
    (id, email, username, password_hash, reset_token, reset_token_expires)
    or None if no matching user is found.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, email, username, password_hash, reset_token, reset_token_expires
                FROM users
                WHERE email = %s OR username = %s
                LIMIT 1;
            """, (email_or_username, email_or_username))
            row = cur.fetchone()
            return row
    finally:
        conn.close()

def find_user_by_email(email: str):
    """
    Retrieve a user row by email.
    Returns a tuple:
    (id, email, username, password_hash, reset_token, reset_token_expires)
    or None if not found.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, email, username, password_hash, reset_token, reset_token_expires
                FROM users
                WHERE email = %s
                LIMIT 1;
            """, (email,))
            return cur.fetchone()
    finally:
        conn.close()

def set_reset_token(user_id: int):
    """
    Generates a secure reset token, sets a 1-hour expiration, and updates the user's record.
    Returns the new reset token.
    """
    token = secrets.token_urlsafe(32)
    expires = datetime.now(timezone.utc) + timedelta(hours=1)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE users
                SET reset_token = %s, reset_token_expires = %s
                WHERE id = %s
            """, (token, expires, user_id))
        conn.commit()
    finally:
        conn.close()
    
    return token
