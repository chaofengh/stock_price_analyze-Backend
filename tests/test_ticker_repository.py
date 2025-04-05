# tests/test_ticker_repository.py
from unittest.mock import patch, MagicMock
from database.ticker_repository import (
    get_all_tickers,
    add_ticker_to_user_list,
    remove_ticker_from_user_list,
)

@patch("database.ticker_repository.get_connection")
def test_get_all_tickers(mock_conn):
    mock_cursor = MagicMock()
    mock_conn.return_value.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [("TSLA",), ("AAPL",)]

    tickers = get_all_tickers()
    assert tickers == ["TSLA", "AAPL"]

    mock_cursor.execute.assert_called_once_with("SELECT symbol FROM tickers;")
    mock_conn.return_value.close.assert_called_once()


@patch("database.ticker_repository.get_connection")
def test_add_ticker_to_user_list_existing_default(mock_conn):
    mock_cursor = MagicMock()
    # Simulate the following:
    # 1. After selecting the ticker id: return (1,)
    # 2. After selecting the default list: return (10,)
    mock_cursor.fetchone.side_effect = [(1,), (10,)]
    mock_conn.return_value.cursor.return_value.__enter__.return_value = mock_cursor

    add_ticker_to_user_list(1, "TSLA")

    insert_ticker_sql = """
                INSERT INTO tickers (symbol)
                VALUES (%s)
                ON CONFLICT (symbol) DO NOTHING
            """
    select_default_list_sql = """
                SELECT id FROM lists
                WHERE user_id = %s AND is_default = TRUE
                LIMIT 1
            """
    insert_list_ticker_sql = """
                INSERT INTO list_tickers (list_id, ticker_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """

    # Verify the ticker is inserted (or ensured to exist)
    mock_cursor.execute.assert_any_call(insert_ticker_sql, ("TSLA",))
    # Verify that the ticker id is selected
    mock_cursor.execute.assert_any_call("SELECT id FROM tickers WHERE symbol = %s", ("TSLA",))
    # Verify that the user's default list id is selected
    mock_cursor.execute.assert_any_call(select_default_list_sql, (1,))
    # Verify that the ticker is linked to the user's default list
    mock_cursor.execute.assert_any_call(insert_list_ticker_sql, (10, 1))

    mock_conn.return_value.commit.assert_called_once()
    mock_conn.return_value.close.assert_called_once()


@patch("database.ticker_repository.get_connection")
def test_remove_ticker_from_user_list(mock_conn):
    mock_cursor = MagicMock()
    # Simulate the following:
    # 1. After selecting the user's default list id: return (10,)
    # 2. After selecting the ticker id: return (1,)
    mock_cursor.fetchone.side_effect = [(10,), (1,)]
    mock_conn.return_value.cursor.return_value.__enter__.return_value = mock_cursor

    remove_ticker_from_user_list(1, "TSLA")

    select_default_list_sql = """
                SELECT id FROM lists
                WHERE user_id = %s AND is_default = TRUE
                LIMIT 1
            """
    delete_list_ticker_sql = """
                DELETE FROM list_tickers
                WHERE list_id = %s AND ticker_id = %s
            """

    # Verify that the default list id is selected for the user
    mock_cursor.execute.assert_any_call(select_default_list_sql, (1,))
    # Verify that the ticker id is selected
    mock_cursor.execute.assert_any_call("SELECT id FROM tickers WHERE symbol = %s", ("TSLA",))
    # Verify that the ticker is removed from the user's list
    mock_cursor.execute.assert_any_call(delete_list_ticker_sql, (10, 1))

    mock_conn.return_value.commit.assert_called_once()
    mock_conn.return_value.close.assert_called_once()
