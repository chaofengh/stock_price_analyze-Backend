# tests/test_ticker_repository.py
from unittest.mock import patch, MagicMock
from database.ticker_repository import get_all_tickers, insert_tickers, remove_ticker

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
def test_insert_tickers(mock_conn):
    mock_cursor = MagicMock()
    mock_conn.return_value.cursor.return_value.__enter__.return_value = mock_cursor

    insert_tickers(["TSLA", "AAPL"])
    # We expect two INSERT statements
    assert mock_cursor.execute.call_count == 2
    # Example of checking one call
    mock_cursor.execute.assert_any_call(
        """
                    INSERT INTO tickers (symbol)
                    VALUES (%s)
                    ON CONFLICT (symbol) DO NOTHING
                """, ("TSLA",)
    )
    mock_conn.return_value.commit.assert_called_once()
    mock_conn.return_value.close.assert_called_once()

@patch("database.ticker_repository.get_connection")
def test_remove_ticker(mock_conn):
    mock_cursor = mock_conn.return_value.cursor.return_value.__enter__.return_value

    remove_ticker("TSLA")
    mock_cursor.execute.assert_called_once_with("DELETE FROM tickers WHERE symbol = %s", ("TSLA",))
    mock_conn.return_value.commit.assert_called_once()
    mock_conn.return_value.close.assert_called_once()
