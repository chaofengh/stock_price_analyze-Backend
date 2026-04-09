from datetime import datetime
from unittest.mock import patch

import pytest
import pytz

import tasks.daily_scan_tasks as daily_scan_tasks

_CHICAGO_TZ = pytz.timezone("America/Chicago")


def _chi_dt(year: int, month: int, day: int, hour: int, minute: int, second: int = 0):
    return _CHICAGO_TZ.localize(datetime(year, month, day, hour, minute, second))


@pytest.fixture(autouse=True)
def _reset_scan_state():
    daily_scan_tasks._reset_scan_state_for_tests()
    yield
    daily_scan_tasks._reset_scan_state_for_tests()


def test_get_latest_scan_result_bootstrap_then_cache_hit():
    now = _chi_dt(2026, 4, 6, 9, 40)
    next_run = _chi_dt(2026, 4, 6, 10, 5)
    payload = {"timestamp": "2026-04-06 09:40:00", "alerts": [{"symbol": "AAPL"}]}

    with (
        patch("tasks.daily_scan_tasks._now_chi", return_value=now),
        patch("tasks.daily_scan_tasks._next_run_time_chi", return_value=next_run),
        patch("tasks.daily_scan_tasks._is_within_regular_session", return_value=True),
        patch("tasks.daily_scan_tasks.daily_scan", return_value=payload) as mock_daily_scan,
    ):
        first = daily_scan_tasks.get_latest_scan_result()
        second = daily_scan_tasks.get_latest_scan_result()

    assert mock_daily_scan.call_count == 1
    assert first["alerts"] == [{"symbol": "AAPL"}]
    assert second["alerts"] == [{"symbol": "AAPL"}]
    assert second["meta"]["next_run_at"] == "2026-04-06 10:05:00"
    assert second["meta"]["is_official"] is True


def test_cached_reads_do_not_recompute_scan():
    warm_now = _chi_dt(2026, 4, 6, 8, 45)
    warm_next = _chi_dt(2026, 4, 6, 9, 5)

    with (
        patch("tasks.daily_scan_tasks._now_chi", return_value=warm_now),
        patch("tasks.daily_scan_tasks._next_run_time_chi", return_value=warm_next),
        patch("tasks.daily_scan_tasks._is_within_regular_session", return_value=True),
        patch(
            "tasks.daily_scan_tasks.daily_scan",
            return_value={"timestamp": "2026-04-06 08:45:00", "alerts": []},
        ),
    ):
        daily_scan_tasks.get_latest_scan_result()

    with (
        patch("tasks.daily_scan_tasks._now_chi", return_value=warm_now),
        patch("tasks.daily_scan_tasks._next_run_time_chi", return_value=warm_next),
        patch("tasks.daily_scan_tasks.daily_scan", side_effect=RuntimeError("should not run")) as mock_scan,
    ):
        cached = daily_scan_tasks.get_latest_scan_result()

    mock_scan.assert_not_called()
    assert cached["alerts"] == []
    assert cached["meta"]["next_run_at"] == "2026-04-06 09:05:00"


def test_daily_scan_wrapper_skips_when_market_closed():
    now = _chi_dt(2026, 4, 5, 12, 0)  # Sunday
    next_run = _chi_dt(2026, 4, 6, 8, 35)

    with (
        patch("tasks.daily_scan_tasks._now_chi", return_value=now),
        patch("tasks.daily_scan_tasks._is_within_regular_session", return_value=False),
        patch("tasks.daily_scan_tasks._next_run_time_chi", return_value=next_run),
        patch("tasks.daily_scan_tasks.daily_scan") as mock_daily_scan,
    ):
        result = daily_scan_tasks.daily_scan_wrapper()

    mock_daily_scan.assert_not_called()
    assert result["status"] == "skipped"
    assert result["reason"] == "market_closed"
    assert result["next_run_at"] == "2026-04-06 08:35:00"
    assert daily_scan_tasks.scan_updated_evt.is_set() is False


def test_daily_scan_wrapper_runs_once_per_slot():
    now = _chi_dt(2026, 4, 6, 10, 35)
    next_run = _chi_dt(2026, 4, 6, 11, 5)
    payload = {"timestamp": "2026-04-06 10:35:00", "alerts": [{"symbol": "MSFT"}]}

    with (
        patch("tasks.daily_scan_tasks._now_chi", return_value=now),
        patch("tasks.daily_scan_tasks._is_within_regular_session", return_value=True),
        patch("tasks.daily_scan_tasks._next_run_time_chi", return_value=next_run),
        patch("tasks.daily_scan_tasks.daily_scan", return_value=payload) as mock_daily_scan,
    ):
        first = daily_scan_tasks.daily_scan_wrapper()
        second = daily_scan_tasks.daily_scan_wrapper()

    assert mock_daily_scan.call_count == 1
    assert first["alerts"] == [{"symbol": "MSFT"}]
    assert second["status"] == "skipped"
    assert second["reason"] == "already_scanned_this_slot"
    assert second["slot"] == "2026-04-06 10:35"
    assert daily_scan_tasks.scan_updated_evt.is_set() is True


def test_wrapper_error_keeps_existing_cache():
    warm_now = _chi_dt(2026, 4, 6, 9, 35)
    warm_next = _chi_dt(2026, 4, 6, 10, 5)
    warm_payload = {"timestamp": "2026-04-06 09:35:00", "alerts": [{"symbol": "TSLA"}]}

    with (
        patch("tasks.daily_scan_tasks._now_chi", return_value=warm_now),
        patch("tasks.daily_scan_tasks._next_run_time_chi", return_value=warm_next),
        patch("tasks.daily_scan_tasks._is_within_regular_session", return_value=True),
        patch("tasks.daily_scan_tasks.daily_scan", return_value=warm_payload),
    ):
        daily_scan_tasks.get_latest_scan_result(force=True)

    failing_now = _chi_dt(2026, 4, 6, 10, 35)
    failing_next = _chi_dt(2026, 4, 6, 11, 5)
    with (
        patch("tasks.daily_scan_tasks._now_chi", return_value=failing_now),
        patch("tasks.daily_scan_tasks._next_run_time_chi", return_value=failing_next),
        patch("tasks.daily_scan_tasks._is_within_regular_session", return_value=True),
        patch("tasks.daily_scan_tasks.daily_scan", side_effect=RuntimeError("boom")),
    ):
        wrapper_result = daily_scan_tasks.daily_scan_wrapper()
        latest = daily_scan_tasks.get_latest_scan_result()

    assert wrapper_result["status"] == "error"
    assert wrapper_result["reason"] == "scan_failed"
    assert latest["alerts"] == [{"symbol": "TSLA"}]
    assert latest["meta"]["next_run_at"] == "2026-04-06 11:05:00"


def test_next_run_time_chi_handles_session_boundary_weekend_and_holiday():
    in_session = _chi_dt(2026, 4, 6, 10, 10)
    next_slot = daily_scan_tasks._next_run_time_chi(in_session)
    assert next_slot.strftime("%Y-%m-%d %H:%M:%S") == "2026-04-06 10:35:00"

    after_first_slot = _chi_dt(2026, 4, 6, 10, 36)
    next_half_hour_slot = daily_scan_tasks._next_run_time_chi(after_first_slot)
    assert next_half_hour_slot.strftime("%Y-%m-%d %H:%M:%S") == "2026-04-06 11:05:00"

    friday_after_close = _chi_dt(2026, 4, 10, 15, 10)
    monday_slot = daily_scan_tasks._next_run_time_chi(friday_after_close)
    assert monday_slot.strftime("%Y-%m-%d %H:%M:%S") == "2026-04-13 08:35:00"

    pre_new_year_holiday = _chi_dt(2025, 12, 31, 15, 10)
    post_holiday_slot = daily_scan_tasks._next_run_time_chi(pre_new_year_holiday)
    assert post_holiday_slot.strftime("%Y-%m-%d %H:%M:%S") == "2026-01-02 08:35:00"
