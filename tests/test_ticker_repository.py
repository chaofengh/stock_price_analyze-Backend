# tests/test_ticker_repository.py
import uuid
from database.ticker_repository import (
    get_all_tickers,
    add_ticker_to_user_list,
    remove_ticker_from_user_list,
)

def _random_symbol():
    return ("T" + uuid.uuid4().hex[:9]).upper()

def _create_user(db_connection):
    suffix = uuid.uuid4().hex[:8]
    email = f"ticker_test_{suffix}@example.com"
    username = f"ticker_test_{suffix}"
    with db_connection.cursor() as cur:
        cur.execute(
            """
                INSERT INTO users (email, username, password_hash)
                VALUES (%s, %s, %s)
                RETURNING id;
            """,
            (email, username, "test_hash"),
        )
        user_id = cur.fetchone()[0]
    db_connection.commit()
    return user_id

def _cleanup_user(db_connection, user_id):
    with db_connection.cursor() as cur:
        cur.execute(
            """
                DELETE FROM list_tickers
                WHERE list_id IN (SELECT id FROM lists WHERE user_id = %s)
            """,
            (user_id,),
        )
        cur.execute("DELETE FROM lists WHERE user_id = %s", (user_id,))
        cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
    db_connection.commit()

def _cleanup_ticker(db_connection, symbol):
    with db_connection.cursor() as cur:
        cur.execute("SELECT id FROM tickers WHERE symbol = %s", (symbol,))
        row = cur.fetchone()
        if row:
            ticker_id = row[0]
            cur.execute("DELETE FROM list_tickers WHERE ticker_id = %s", (ticker_id,))
            cur.execute("DELETE FROM tickers WHERE id = %s", (ticker_id,))
    db_connection.commit()

def test_get_all_tickers(db_connection):
    symbol_one = _random_symbol()
    symbol_two = _random_symbol()
    try:
        with db_connection.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO tickers (symbol)
                    VALUES (%s), (%s)
                    ON CONFLICT (symbol) DO NOTHING;
                """,
                (symbol_one, symbol_two),
            )
        db_connection.commit()

        tickers = get_all_tickers()
        assert symbol_one in tickers
        assert symbol_two in tickers
    finally:
        _cleanup_ticker(db_connection, symbol_one)
        _cleanup_ticker(db_connection, symbol_two)


def test_add_ticker_to_user_list_existing_default(db_connection):
    user_id = _create_user(db_connection)
    symbol = _random_symbol()
    list_id = None
    try:
        with db_connection.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO lists (user_id, name, is_default)
                    VALUES (%s, %s, TRUE)
                    RETURNING id;
                """,
                (user_id, "Default List"),
            )
            list_id = cur.fetchone()[0]
        db_connection.commit()

        add_ticker_to_user_list(user_id, symbol)

        with db_connection.cursor() as cur:
            cur.execute("SELECT id FROM tickers WHERE symbol = %s", (symbol,))
            ticker_id = cur.fetchone()[0]
            cur.execute(
                """
                    SELECT 1
                    FROM list_tickers
                    WHERE list_id = %s AND ticker_id = %s
                """,
                (list_id, ticker_id),
            )
            assert cur.fetchone() is not None
    finally:
        _cleanup_user(db_connection, user_id)
        _cleanup_ticker(db_connection, symbol)


def test_remove_ticker_from_user_list(db_connection):
    user_id = _create_user(db_connection)
    symbol = _random_symbol()
    try:
        add_ticker_to_user_list(user_id, symbol)

        with db_connection.cursor() as cur:
            cur.execute(
                """
                    SELECT id
                    FROM lists
                    WHERE user_id = %s AND is_default = TRUE
                    LIMIT 1
                """,
                (user_id,),
            )
            list_id = cur.fetchone()[0]
            cur.execute("SELECT id FROM tickers WHERE symbol = %s", (symbol,))
            ticker_id = cur.fetchone()[0]

        remove_ticker_from_user_list(user_id, symbol)

        with db_connection.cursor() as cur:
            cur.execute(
                """
                    SELECT 1
                    FROM list_tickers
                    WHERE list_id = %s AND ticker_id = %s
                """,
                (list_id, ticker_id),
            )
            assert cur.fetchone() is None
    finally:
        _cleanup_user(db_connection, user_id)
        _cleanup_ticker(db_connection, symbol)
