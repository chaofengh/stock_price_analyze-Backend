import secrets
from datetime import datetime, timezone, timedelta
from typing import Optional
import bcrypt
from .connection import get_connection

_BCRYPT_ROUNDS = 12

def hash_password(plain_password: str) -> str:
    """
    Hash the plain-text password using bcrypt.
    A unique salt is generated automatically.
    """
    if not isinstance(plain_password, str) or not plain_password:
        raise ValueError("Password must be a non-empty string")
    salt = bcrypt.gensalt(rounds=_BCRYPT_ROUNDS)
    hashed = bcrypt.hashpw(plain_password.encode("utf-8"), salt)
    return hashed.decode("utf-8")

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verify a plain-text password against the hashed (and salted) password.
    """
    if not isinstance(plain_password, str) or not isinstance(hashed_password, str):
        return False
    if not plain_password or not hashed_password:
        return False
    try:
        return bcrypt.checkpw(plain_password.encode("utf-8"), hashed_password.encode("utf-8"))
    except ValueError:
        return False

def create_user(
    email: str,
    username: str,
    password: str,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
    phone: Optional[str] = None,
    country: Optional[str] = None,
    timezone: Optional[str] = None,
    marketing_opt_in: bool = False,
):
    """
    Inserts a new user into the database with a hashed (and salted) password.
    Returns the newly created user row.
    """
    hashed = hash_password(password)
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (
                    email,
                    username,
                    password_hash,
                    first_name,
                    last_name,
                    phone,
                    country,
                    timezone,
                    marketing_opt_in
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING
                    id,
                    email,
                    username,
                    created_at,
                    first_name,
                    last_name,
                    phone,
                    country,
                    timezone,
                    marketing_opt_in;
            """, (email, username, hashed, first_name, last_name, phone, country, timezone, marketing_opt_in))
            user_row = cur.fetchone()
        conn.commit()
        return user_row
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def get_user_public_profile(user_id: int):
    """
    Returns a tuple:
    (id, email, username, created_at, first_name, last_name, phone, country, timezone, marketing_opt_in)
    or None if not found.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    email,
                    username,
                    created_at,
                    first_name,
                    last_name,
                    phone,
                    country,
                    timezone,
                    marketing_opt_in
                FROM users
                WHERE id = %s
                LIMIT 1;
                """,
                (user_id,),
            )
            return cur.fetchone()
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
