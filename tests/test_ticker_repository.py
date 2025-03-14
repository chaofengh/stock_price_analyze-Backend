# tests/test_ticker_repository.py

import pytest
from unittest.mock import patch, MagicMock
from database.ticker_repository import get_all_tickers, insert_tickers, remove_ticker

@patch('database.ticker_repository.get_connection')
def test_get_all_tickers(mock_conn):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [('AAPL',), ('TSLA',)]
    mock_conn.return_value.__enter__.return_value.cursor.return_value = mock_cursor

    tickers = get_all_tickers()
    assert tickers == ['AAPL', 'TSLA']

@patch('database.ticker_repository.get_connection')
def test_insert_tickers(mock_conn):
    mock_cursor = MagicMock()
    mock_conn.return_value.__enter__.return_value.cursor.return_value = mock_cursor

    insert_tickers(['AAPL', 'TSLA'])
    # Check if the insert statement was called for each symbol
    assert mock_cursor.execute.call_count == 2

@patch('database.ticker_repository.get_connection')
def test_remove_ticker(mock_conn):
    mock_cursor = MagicMock()
    mock_conn.return_value.__enter__.return_value.cursor.return_value = mock_cursor

    remove_ticker('AAPL')
    mock_cursor.execute.assert_called_once()
