# tests/test_watchlist_cache_tasks.py
import pandas as pd
from unittest.mock import patch

from tasks.watchlist_cache_tasks import (
    _filter_latest_trading_session,
    _get_prev_close_from_daily,
    refresh_watchlist_cache,
)


def _intraday_df(rows):
    return pd.DataFrame(rows)


def _daily_df(rows):
    return pd.DataFrame(rows)


def test_filter_latest_trading_session_returns_latest_day_and_prev_close():
    df = _intraday_df(
        [
            {"date": "2024-05-01 09:30:00", "close": 100.0},
            {"date": "2024-05-01 10:00:00", "close": 101.5},
            {"date": "2024-05-02 09:30:00", "close": 102.0},
            {"date": "2024-05-02 10:00:00", "close": 103.0},
        ]
    )

    latest_df, prev_close = _filter_latest_trading_session(df)
    assert prev_close == 101.5
    assert len(latest_df) == 2
    assert pd.to_datetime(latest_df["date"]).dt.date.nunique() == 1
    assert latest_df["close"].tolist() == [102.0, 103.0]


def test_get_prev_close_from_daily_prefers_prior_close():
    daily_df = _daily_df(
        [
            {"date": "2024-05-01", "close": 97.0},
            {"date": "2024-05-02", "close": 99.0},
        ]
    )
    assert _get_prev_close_from_daily(daily_df) == 97.0


def test_refresh_watchlist_cache_updates_using_daily_prev_close():
    symbols = ["AAPL", "MSFT"]
    intraday_data = {
        "AAPL": _intraday_df(
            [
                {"date": "2024-05-01 09:30:00", "close": 184.0},
                {"date": "2024-05-01 10:00:00", "close": 185.0},
                {"date": "2024-05-02 09:30:00", "close": 186.0},
            ]
        ),
        "MSFT": _intraday_df(
            [
                {"date": "2024-05-02 09:30:00", "close": 312.0},
                {"date": "2024-05-02 10:00:00", "close": 313.0},
            ]
        ),
    }
    daily_data = {
        "MSFT": _daily_df(
            [
                {"date": "2024-05-01", "close": 309.0},
                {"date": "2024-05-02", "close": 313.0},
            ]
        )
    }

    fetch_calls = []

    def _fetch_side_effect(requested_symbols, period, interval, threads=False):
        fetch_calls.append((tuple(requested_symbols), period, interval, threads))
        if interval == "5m":
            return intraday_data
        return daily_data

    with patch(
        "tasks.watchlist_cache_tasks.get_symbols_for_price_movement_update",
        return_value=symbols,
    ), patch(
        "tasks.watchlist_cache_tasks.fetch_stock_data",
        side_effect=_fetch_side_effect,
    ), patch(
        "tasks.watchlist_cache_tasks.upsert_price_movement_data"
    ) as mock_upsert:
        result = refresh_watchlist_cache(batch_size=10)

    assert result["updated"] == 2
    assert set(result["symbols"]) == set(symbols)
    assert fetch_calls[0] == (tuple(symbols), "5d", "5m", False)
    assert fetch_calls[1] == (("MSFT",), "7d", "1d", False)

    assert mock_upsert.call_count == 2
    for symbol, payload in (call.args for call in mock_upsert.call_args_list):
        assert symbol in symbols
        assert "candles" in payload
        assert "summary" in payload

    payload_by_symbol = {call.args[0]: call.args[1] for call in mock_upsert.call_args_list}
    assert payload_by_symbol["AAPL"]["summary"]["previousClose"] == 185.0
    assert payload_by_symbol["MSFT"]["summary"]["previousClose"] == 309.0

    aapl_dates = pd.to_datetime([row["date"] for row in payload_by_symbol["AAPL"]["candles"]]).date
    assert set(aapl_dates) == {pd.Timestamp("2024-05-02").date()}
