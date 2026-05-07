import os
import time
from unittest.mock import call, patch

import tasks.daily_scan_tasks as daily_scan_tasks
import tasks.entry_decision_preload_tasks as preload_tasks


def setup_function():
    daily_scan_tasks._reset_scan_state_for_tests()
    preload_tasks._reset_entry_decision_preload_state_for_tests()


def teardown_function():
    daily_scan_tasks._reset_scan_state_for_tests()
    preload_tasks._reset_entry_decision_preload_state_for_tests()


def test_alert_symbols_from_payload_normalizes_and_dedupes():
    payload = {
        "alerts": [
            {"symbol": "amd"},
            {"ticker": "MU"},
            {"symbol": "AMD"},
            "nvda",
            {},
        ]
    }

    assert preload_tasks.alert_symbols_from_payload(payload) == ["AMD", "MU", "NVDA"]


def test_default_worker_timeout_allows_realistic_cold_entry_model_runtime():
    assert preload_tasks.DEFAULT_WORKER_TIMEOUT_SECONDS >= 180


def test_preload_skips_without_cached_alert_scan():
    with patch("tasks.entry_decision_preload_tasks._compute_preload_artifacts") as mock_compute:
        result = preload_tasks.preload_entry_decisions_from_latest_alerts(
            max_symbols=1,
            min_idle_seconds=0,
        )

    assert result["status"] == "skipped"
    assert result["reason"] == "no_alert_symbols"
    mock_compute.assert_not_called()


def test_preload_loads_one_alert_symbol_per_idle_pass():
    daily_scan_tasks._store_cached_result(
        {
            "timestamp": "2026-04-15 10:05:00",
            "alerts": [{"symbol": "AMD"}, {"symbol": "MU"}],
        }
    )

    def _entry_payloads(symbols, as_of_date):
        loaded = {
            symbols[0]: {
                "symbol": symbols[0],
                "requested_as_of_date": as_of_date,
                "as_of_date": as_of_date,
                "horizons": {},
                "top_reasons": [],
                "backtest_1y": {},
                "chart_data": [],
            }
        }
        contexts = {symbols[0]: {"symbol": symbols[0], "feature_df": object(), "decisions_by_index": {}, "backtest_1y": {}}}
        return loaded, contexts, {}

    with (
        patch.dict(os.environ, {"ENTRY_DECISION_PRELOAD_FULL_ENABLED": "1", "ENTRY_DECISION_PRELOAD_INLINE": "1"}),
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch(
            "tasks.entry_decision_preload_tasks._compute_preload_artifacts",
            side_effect=_entry_payloads,
        ) as mock_compute,
    ):
        first = preload_tasks.preload_entry_decisions_from_latest_alerts(
            max_symbols=1,
            min_idle_seconds=0,
        )
        second = preload_tasks.preload_entry_decisions_from_latest_alerts(
            max_symbols=1,
            min_idle_seconds=0,
        )

        assert first["status"] == "loaded"
        assert first["symbols"] == ["AMD"]
        assert second["status"] == "loaded"
        assert second["symbols"] == ["MU"]
        assert [call.args[0] for call in mock_compute.call_args_list] == [["AMD"], ["MU"]]
        assert [call.args[1] for call in mock_compute.call_args_list] == [
            "2026-04-15",
            "2026-04-15",
        ]
        assert preload_tasks.get_preloaded_entry_decision("amd", as_of_date="2026-04-15")["symbol"] == "AMD"
        assert preload_tasks.get_preloaded_entry_decision("MU", as_of_date="2026-04-15")["symbol"] == "MU"


def test_preload_skips_without_snapshot_when_full_preload_disabled():
    daily_scan_tasks._store_cached_result(
        {
            "timestamp": "2026-04-15 10:05:00",
            "alerts": [{"symbol": "AMD", "touched_side": "Lower"}],
        }
    )

    with (
        patch.dict(os.environ, {"ENTRY_DECISION_PRELOAD_FULL_ENABLED": "0"}),
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks._compute_preload_artifacts") as mock_compute,
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        result = preload_tasks.preload_entry_decisions_from_latest_alerts(
            max_symbols=1,
            min_idle_seconds=0,
        )

    assert result == {
        "status": "skipped",
        "reason": "full_preload_disabled",
        "symbols": ["AMD"],
    }
    assert preload_tasks.get_preloaded_entry_decision("AMD", as_of_date="2026-04-15") is None
    mock_compute.assert_not_called()
    mock_start.assert_not_called()


def test_preload_uses_batched_price_data_without_request_compute_path():
    def _frame():
        import pandas as pd

        dates = pd.date_range("2025-01-01", periods=80, freq="B")
        return pd.DataFrame(
            {
                "date": dates,
                "open": [100.0] * len(dates),
                "high": [102.0] * len(dates),
                "low": [98.0] * len(dates),
                "close": [100.0] * len(dates),
                "volume": [1000] * len(dates),
                "BB_upper": [101.0] * len(dates),
                "BB_lower": [99.0] * len(dates),
                "BB_middle": [100.0] * len(dates),
            }
        )

    context = {"symbol": "AMD", "feature_df": object(), "decisions_by_index": {}, "backtest_1y": {}}
    with (
        patch(
            "tasks.entry_decision_preload_tasks.prepare_stock_data",
            return_value={"AMD": _frame(), "QQQ": _frame(), "XLK": _frame()},
        ) as mock_prepare,
        patch(
            "tasks.entry_decision_preload_tasks.build_entry_decision_context_from_frame",
            return_value=context,
        ) as mock_build_context,
        patch(
            "tasks.entry_decision_preload_tasks.build_entry_decision_from_context",
            return_value={"symbol": "AMD", "requested_as_of_date": "2026-04-15", "as_of_date": "2026-04-15"},
        ) as mock_build_payload,
    ):
        loaded, failed = preload_tasks._compute_preload_payloads(["AMD"], "2026-04-15")

    assert failed == {}
    assert loaded["AMD"]["symbol"] == "AMD"
    mock_prepare.assert_called_once()
    assert mock_prepare.call_args.args[0] == ["AMD", "QQQ", "XLK"]
    assert mock_prepare.call_args.kwargs["period"] == "2y"
    assert mock_prepare.call_args.kwargs["threads"] is False
    mock_build_context.assert_called_once()
    assert mock_build_context.call_args.kwargs["earnings_dates"] == set()
    assert set(mock_build_context.call_args.kwargs["context_frames"].keys()) == {"QQQ", "XLK"}
    mock_build_payload.assert_called_once_with(context, as_of_date="2026-04-15")


def test_preload_enters_global_backoff_when_price_source_returns_no_frames():
    daily_scan_tasks._store_cached_result(
        {"timestamp": "2026-04-15 10:05:00", "alerts": [{"symbol": "AMD"}, {"symbol": "MU"}]}
    )

    with (
        patch.dict(os.environ, {"ENTRY_DECISION_PRELOAD_FULL_ENABLED": "1", "ENTRY_DECISION_PRELOAD_INLINE": "1"}),
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch(
            "tasks.entry_decision_preload_tasks._compute_preload_artifacts",
            return_value=({}, {}, {"AMD": "no data", "__global__": "no data"}),
        ) as mock_compute,
    ):
        first = preload_tasks.preload_entry_decisions_from_latest_alerts(
            max_symbols=1,
            min_idle_seconds=0,
        )
        second = preload_tasks.preload_entry_decisions_from_latest_alerts(
            max_symbols=1,
            min_idle_seconds=0,
        )

    assert first["status"] == "error"
    assert first["failed"] == {"AMD": "no data"}
    assert second["status"] == "skipped"
    assert second["reason"] == "preload_source_backoff"
    assert mock_compute.call_count == 1


def test_preload_scheduler_starts_worker_and_returns_immediately():
    daily_scan_tasks._store_cached_result(
        {"timestamp": "2026-04-15 10:05:00", "alerts": [{"symbol": "AMD"}]}
    )

    with (
        patch.dict(os.environ, {"ENTRY_DECISION_PRELOAD_FULL_ENABLED": "1"}),
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
        patch("tasks.entry_decision_preload_tasks._compute_preload_artifacts") as mock_compute,
    ):
        result = preload_tasks.preload_entry_decisions_from_latest_alerts(
            max_symbols=1,
            min_idle_seconds=0,
        )

    assert result == {"status": "started", "symbols": ["AMD"]}
    mock_start.assert_called_once_with(["AMD"], "2026-04-15", source="background")
    mock_compute.assert_not_called()


def test_preload_scheduler_default_batch_warms_two_alert_symbols():
    daily_scan_tasks._store_cached_result(
        {
            "timestamp": "2026-04-15 10:05:00",
            "alerts": [{"symbol": "AMD"}, {"symbol": "MU"}, {"symbol": "AAPL"}],
        }
    )

    with (
        patch.dict(os.environ, {"ENTRY_DECISION_PRELOAD_FULL_ENABLED": "1"}),
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        result = preload_tasks.preload_entry_decisions_from_latest_alerts(min_idle_seconds=0)

    assert result == {"status": "started", "symbols": ["AMD", "MU"]}
    mock_start.assert_called_once_with(["AMD", "MU"], "2026-04-15", source="background")


def test_user_alert_payload_preload_starts_robust_alert_worker_immediately():
    payload = {
        "timestamp": "2026-04-15 10:05:00",
        "alerts": [{"symbol": "UBER"}, {"symbol": "AMD"}],
    }

    with (
        patch.dict(os.environ, {"ENTRY_DECISION_PRELOAD_FULL_ENABLED": "1"}),
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        result = preload_tasks.preload_entry_decisions_from_alert_payload(payload, min_idle_seconds=0)

    assert result["status"] == "started"
    assert result["source"] == "alert"
    assert result["symbols"] == ["UBER", "AMD"]
    assert result["queued_symbols"] == ["UBER", "AMD"]
    assert result["queued_count"] == 2
    mock_start.assert_called_once_with(["UBER", "AMD"], "2026-04-15", source="alert")


def test_user_alert_payload_preload_inline_stores_reusable_context_for_date_changes():
    payload = {"timestamp": "2026-04-15 10:05:00", "alerts": [{"symbol": "UBER"}]}
    context = {"symbol": "UBER", "feature_df": object(), "decisions_by_index": {}, "backtest_1y": {}}

    with (
        patch.dict(
            os.environ,
            {"ENTRY_DECISION_PRELOAD_FULL_ENABLED": "1", "ENTRY_DECISION_PRELOAD_INLINE": "1"},
        ),
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch(
            "tasks.entry_decision_preload_tasks.get_entry_decision_context",
            return_value=context,
        ) as mock_get_context,
        patch(
            "tasks.entry_decision_preload_tasks.build_entry_decision_from_context",
            side_effect=lambda _context, as_of_date=None: {
                "symbol": "UBER",
                "requested_as_of_date": as_of_date,
                "as_of_date": as_of_date,
                "horizons": {},
                "backtest_1y": {},
                "chart_data": [],
            },
        ) as mock_build_payload,
        patch("tasks.entry_decision_preload_tasks._compute_preload_artifacts") as mock_lightweight,
    ):
        result = preload_tasks.preload_entry_decisions_from_alert_payload(payload, min_idle_seconds=0)
        selected_date_payload = preload_tasks.get_preloaded_entry_decision(
            "UBER",
            as_of_date="2026-04-10",
            full_only=True,
        )

    assert result["status"] == "loaded"
    assert result["symbols"] == ["UBER"]
    assert selected_date_payload["requested_as_of_date"] == "2026-04-10"
    mock_get_context.assert_called_once_with("UBER")
    assert mock_build_payload.call_args_list == [
        call(context, as_of_date="2026-04-15"),
        call(context, as_of_date="2026-04-10"),
    ]
    mock_lightweight.assert_not_called()


def test_user_alert_payload_preload_uses_alert_timestamp_as_request_date():
    payload = {"timestamp": "2026-04-14 15:35:00", "alerts": [{"symbol": "UBER"}]}
    context = {"symbol": "UBER", "feature_df": object(), "decisions_by_index": {}, "backtest_1y": {}}

    with (
        patch.dict(
            os.environ,
            {"ENTRY_DECISION_PRELOAD_FULL_ENABLED": "1", "ENTRY_DECISION_PRELOAD_INLINE": "1"},
        ),
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch(
            "tasks.entry_decision_preload_tasks.get_entry_decision_context",
            return_value=context,
        ) as mock_get_context,
        patch(
            "tasks.entry_decision_preload_tasks.build_entry_decision_from_context",
            side_effect=lambda _context, as_of_date=None: {
                "symbol": "UBER",
                "requested_as_of_date": as_of_date,
                "as_of_date": as_of_date,
                "horizons": {},
                "backtest_1y": {},
                "chart_data": [],
            },
        ) as mock_build_payload,
    ):
        result = preload_tasks.preload_entry_decisions_from_alert_payload(payload, min_idle_seconds=0)
        cached_on_alert_date = preload_tasks.get_preloaded_entry_decision("UBER", as_of_date="2026-04-14")
        cached_on_cache_day = preload_tasks.get_preloaded_entry_decision("UBER", as_of_date="2026-04-15")

    assert result["status"] == "loaded"
    assert cached_on_alert_date["symbol"] == "UBER"
    assert cached_on_cache_day["requested_as_of_date"] == "2026-04-15"
    mock_get_context.assert_called_once_with("UBER")
    assert mock_build_payload.call_args_list == [
        call(context, as_of_date="2026-04-14"),
        call(context, as_of_date="2026-04-15"),
    ]


def test_preloaded_context_renders_selected_date_without_new_worker_payload():
    context = {"symbol": "AMD", "feature_df": object(), "decisions_by_index": {}, "backtest_1y": {}}
    with (
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch(
            "tasks.entry_decision_preload_tasks.build_entry_decision_from_context",
            return_value={
                "symbol": "AMD",
                "requested_as_of_date": "2026-04-10",
                "as_of_date": "2026-04-10",
                "horizons": {},
                "backtest_1y": {},
                "chart_data": [],
            },
        ) as mock_build_payload,
    ):
        assert preload_tasks.store_entry_decision_context("AMD", context)
        payload = preload_tasks.get_preloaded_entry_decision("AMD", as_of_date="2026-04-10", full_only=True)

    assert payload["requested_as_of_date"] == "2026-04-10"
    mock_build_payload.assert_called_once_with(context, as_of_date="2026-04-10")


def test_user_alert_payload_queues_all_symbols_when_worker_is_busy():
    class FakeProcess:
        def is_alive(self):
            return True

        def terminate(self):
            return None

        def kill(self):
            return None

        def join(self, timeout=None):
            return timeout

    preload_tasks._worker_process = FakeProcess()
    preload_tasks._worker_started_at = time.monotonic()
    preload_tasks._worker_symbols = ["AAPL"]
    preload_tasks._worker_as_of_date = "2026-04-15"
    preload_tasks._worker_source = "background"

    payload = {
        "timestamp": "2026-04-15 10:05:00",
        "alerts": [{"symbol": "UBER"}, {"symbol": "AMD"}, {"symbol": "MU"}],
    }

    with (
        patch.dict(os.environ, {"ENTRY_DECISION_PRELOAD_FULL_ENABLED": "1"}),
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        result = preload_tasks.preload_entry_decisions_from_alert_payload(payload, min_idle_seconds=0)

    assert result["status"] == "running"
    assert result["source"] == "background"
    assert result["queued_symbols"] == ["UBER", "AMD", "MU"]
    assert result["queued_count"] == 3
    mock_start.assert_not_called()


def test_scheduler_prioritizes_queued_user_alert_symbols_before_background_scan():
    daily_scan_tasks._store_cached_result(
        {
            "timestamp": "2026-04-15 10:05:00",
            "alerts": [{"symbol": "AAPL"}],
        }
    )

    with (
        patch.dict(os.environ, {"ENTRY_DECISION_PRELOAD_FULL_ENABLED": "1"}),
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        preload_tasks._queue_alert_preload_symbols(["UBER", "AMD", "MU"], "2026-04-15")
        result = preload_tasks.preload_entry_decisions_from_latest_alerts(
            max_symbols=2,
            min_idle_seconds=0,
        )

    assert result == {"status": "started", "symbols": ["UBER", "AMD"], "source": "alert"}
    mock_start.assert_called_once_with(["UBER", "AMD"], "2026-04-15", source="alert")


def test_user_alert_payload_moves_visible_symbols_to_front_of_queue():
    with (
        patch.dict(os.environ, {"ENTRY_DECISION_PRELOAD_FULL_ENABLED": "1"}),
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        preload_tasks._queue_alert_preload_symbols(["OLD1", "OLD2"], "2026-04-15")
        result = preload_tasks.preload_entry_decisions_from_alert_payload(
            {"timestamp": "2026-04-15 10:05:00", "alerts": [{"symbol": "UBER"}]},
            max_symbols=1,
            min_idle_seconds=0,
        )

    assert result["status"] == "started"
    assert result["symbols"] == ["UBER"]
    mock_start.assert_called_once_with(["UBER"], "2026-04-15", source="alert")


def test_user_alert_payload_default_batch_leaves_remaining_symbols_queued():
    payload = {
        "timestamp": "2026-04-15 10:05:00",
        "alerts": [{"symbol": "UBER"}, {"symbol": "AMD"}, {"symbol": "MU"}],
    }

    with (
        patch.dict(os.environ, {"ENTRY_DECISION_PRELOAD_FULL_ENABLED": "1"}),
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        result = preload_tasks.preload_entry_decisions_from_alert_payload(payload, min_idle_seconds=0)
        next_batch = preload_tasks._next_queued_alert_batch(max_symbols=2)

    assert result["status"] == "started"
    assert result["symbols"] == ["UBER", "AMD"]
    assert result["queued_symbols"] == ["UBER", "AMD", "MU"]
    assert next_batch == (["MU"], "2026-04-15")
    mock_start.assert_called_once_with(["UBER", "AMD"], "2026-04-15", source="alert")


def test_interactive_preload_force_bypasses_background_disable():
    with (
        patch.dict(os.environ, {"ENTRY_DECISION_PRELOAD_FULL_ENABLED": "0"}),
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        result = preload_tasks.request_full_entry_decision_preload(
            "AMD",
            as_of_date="2026-04-15",
            force=True,
        )

    assert result == {"status": "started", "symbol": "AMD"}
    mock_start.assert_called_once_with(["AMD"], "2026-04-15", source="interactive")


def test_interactive_preload_can_bypass_global_backoff_once_for_symbol():
    preload_tasks._mark_global_preload_failed()

    with (
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        result = preload_tasks.request_full_entry_decision_preload(
            "AMD",
            as_of_date="2026-04-15",
            force=True,
            ignore_backoff=True,
        )

    assert result == {"status": "started", "symbol": "AMD"}
    mock_start.assert_called_once_with(["AMD"], "2026-04-15", source="interactive")


def test_interactive_preload_respects_same_symbol_failure_backoff():
    with (
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        preload_tasks._mark_interactive_preload_failed("AMD", as_of_date="2026-04-15")
        result = preload_tasks.request_full_entry_decision_preload(
            "AMD",
            as_of_date="2026-04-15",
            force=True,
            ignore_backoff=True,
        )

    assert result["status"] == "skipped"
    assert result["reason"] == "interactive_symbol_preload_backoff"
    assert result["retry_after_seconds"] > 0
    mock_start.assert_not_called()


def test_interactive_preload_ignores_background_symbol_backoff():
    with (
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        preload_tasks._mark_preload_failed("SPOT", as_of_date="2026-04-15")
        result = preload_tasks.request_full_entry_decision_preload(
            "SPOT",
            as_of_date="2026-04-15",
            force=True,
            ignore_backoff=True,
        )

    assert result == {"status": "started", "symbol": "SPOT"}
    mock_start.assert_called_once_with(["SPOT"], "2026-04-15", source="interactive")


def test_interactive_preload_preempts_background_worker_for_different_symbol():
    class FakeProcess:
        terminated = False

        def is_alive(self):
            return not self.terminated

        def terminate(self):
            self.terminated = True

        def kill(self):
            self.terminated = True

        def join(self, timeout=None):
            return timeout

    preload_tasks._worker_process = FakeProcess()
    preload_tasks._worker_started_at = time.monotonic()
    preload_tasks._worker_symbols = ["MU"]
    preload_tasks._worker_as_of_date = "2026-04-15"
    preload_tasks._worker_source = "background"

    with (
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        result = preload_tasks.request_full_entry_decision_preload(
            "AMD",
            as_of_date="2026-04-15",
            force=True,
        )

    assert result == {"status": "started", "symbol": "AMD"}
    mock_start.assert_called_once_with(["AMD"], "2026-04-15", source="interactive")


def test_interactive_preload_requeues_preempted_alert_worker_symbols():
    class FakeProcess:
        terminated = False

        def is_alive(self):
            return not self.terminated

        def terminate(self):
            self.terminated = True

        def kill(self):
            self.terminated = True

        def join(self, timeout=None):
            return timeout

    preload_tasks._worker_process = FakeProcess()
    preload_tasks._worker_started_at = time.monotonic()
    preload_tasks._worker_symbols = ["UBER", "AMD"]
    preload_tasks._worker_as_of_date = "2026-04-15"
    preload_tasks._worker_source = "alert"

    with (
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        result = preload_tasks.request_full_entry_decision_preload(
            "SPOT",
            as_of_date="2026-04-15",
            force=True,
        )
        next_batch = preload_tasks._next_queued_alert_batch(max_symbols=2)

    assert result == {"status": "started", "symbol": "SPOT"}
    assert next_batch == (["UBER", "AMD"], "2026-04-15")
    mock_start.assert_called_once_with(["SPOT"], "2026-04-15", source="interactive")


def test_interactive_preload_preempts_alert_batch_even_when_symbol_is_in_batch():
    class FakeProcess:
        terminated = False

        def is_alive(self):
            return not self.terminated

        def terminate(self):
            self.terminated = True

        def kill(self):
            self.terminated = True

        def join(self, timeout=None):
            return timeout

    active_process = FakeProcess()
    preload_tasks._worker_process = active_process
    preload_tasks._worker_started_at = time.monotonic()
    preload_tasks._worker_symbols = ["UBER", "AMD"]
    preload_tasks._worker_as_of_date = "2026-04-15"
    preload_tasks._worker_source = "alert"

    with (
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        result = preload_tasks.request_full_entry_decision_preload(
            "AMD",
            as_of_date="2026-04-15",
            force=True,
        )
        next_batch = preload_tasks._next_queued_alert_batch(max_symbols=2)

    assert result == {"status": "started", "symbol": "AMD"}
    assert not active_process.is_alive()
    assert next_batch == (["UBER", "AMD"], "2026-04-15")
    mock_start.assert_called_once_with(["AMD"], "2026-04-15", source="interactive")


def test_interactive_preload_reuses_single_symbol_alert_worker():
    class FakeProcess:
        def is_alive(self):
            return True

        def terminate(self):
            return None

        def kill(self):
            return None

        def join(self, timeout=None):
            return timeout

    preload_tasks._worker_process = FakeProcess()
    preload_tasks._worker_started_at = time.monotonic()
    preload_tasks._worker_symbols = ["AMD"]
    preload_tasks._worker_as_of_date = "2026-04-15"
    preload_tasks._worker_source = "alert"

    with (
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        result = preload_tasks.request_full_entry_decision_preload(
            "AMD",
            as_of_date="2026-04-15",
            force=True,
        )

    assert result["status"] == "running"
    assert result["symbol"] == "AMD"
    assert result["source"] == "alert"
    mock_start.assert_not_called()


def test_interactive_preload_preempts_background_worker_even_when_symbol_is_in_batch():
    class FakeProcess:
        terminated = False

        def is_alive(self):
            return not self.terminated

        def terminate(self):
            self.terminated = True

        def kill(self):
            self.terminated = True

        def join(self, timeout=None):
            return timeout

    active_process = FakeProcess()
    preload_tasks._worker_process = active_process
    preload_tasks._worker_started_at = time.monotonic()
    preload_tasks._worker_symbols = ["AMD", "MU"]
    preload_tasks._worker_as_of_date = "2026-04-15"
    preload_tasks._worker_source = "background"

    with (
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        result = preload_tasks.request_full_entry_decision_preload(
            "AMD",
            as_of_date="2026-04-15",
            force=True,
        )

    assert result == {"status": "started", "symbol": "AMD"}
    assert not active_process.is_alive()
    mock_start.assert_called_once_with(["AMD"], "2026-04-15", source="interactive")


def test_interactive_preload_preempts_background_worker_despite_background_symbol_backoff():
    class FakeProcess:
        terminated = False

        def is_alive(self):
            return not self.terminated

        def terminate(self):
            self.terminated = True

        def kill(self):
            self.terminated = True

        def join(self, timeout=None):
            return timeout

    active_process = FakeProcess()
    preload_tasks._worker_process = active_process
    preload_tasks._worker_started_at = time.monotonic()
    preload_tasks._worker_symbols = ["MU"]
    preload_tasks._worker_as_of_date = "2026-04-15"
    preload_tasks._worker_source = "background"

    with (
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        preload_tasks._mark_preload_failed("AMD", as_of_date="2026-04-15")
        result = preload_tasks.request_full_entry_decision_preload(
            "AMD",
            as_of_date="2026-04-15",
            force=True,
        )

    assert result == {"status": "started", "symbol": "AMD"}
    assert not active_process.is_alive()
    mock_start.assert_called_once_with(["AMD"], "2026-04-15", source="interactive")


def test_interactive_preload_preempts_stale_interactive_worker_for_different_symbol():
    class FakeProcess:
        terminated = False

        def is_alive(self):
            return not self.terminated

        def terminate(self):
            self.terminated = True

        def kill(self):
            self.terminated = True

        def join(self, timeout=None):
            return timeout

    active_process = FakeProcess()
    preload_tasks._worker_process = active_process
    preload_tasks._worker_started_at = time.monotonic()
    preload_tasks._worker_symbols = ["MU"]
    preload_tasks._worker_as_of_date = "2026-04-15"
    preload_tasks._worker_source = "interactive"

    with (
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        result = preload_tasks.request_full_entry_decision_preload(
            "AMD",
            as_of_date="2026-04-15",
            force=True,
        )

    assert result == {"status": "started", "symbol": "AMD"}
    assert not active_process.is_alive()
    mock_start.assert_called_once_with(["AMD"], "2026-04-15", source="interactive")


def test_interactive_preload_reports_worker_start_failure():
    with (
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks._start_preload_worker", side_effect=RuntimeError("spawn failed")),
    ):
        result = preload_tasks.request_full_entry_decision_preload(
            "AMD",
            as_of_date="2026-04-15",
            force=True,
        )

    assert result["status"] == "error"
    assert result["reason"] == "preload_start_failed"
    assert result["symbol"] == "AMD"
    assert result["retry_after_seconds"] == preload_tasks.DEFAULT_INTERACTIVE_FAILURE_BACKOFF_SECONDS


def test_interactive_worker_returns_reusable_context():
    context = {"symbol": "SPOT", "feature_df": object(), "decisions_by_index": {}, "backtest_1y": {}}
    with (
        patch(
            "tasks.entry_decision_preload_tasks.get_entry_decision_context",
            return_value=context,
        ) as mock_get_context,
        patch(
            "tasks.entry_decision_preload_tasks.build_entry_decision_from_context",
            return_value={"symbol": "SPOT", "horizons": {}, "backtest_1y": {}, "chart_data": []},
        ) as mock_build_payload,
    ):
        loaded, contexts, failed = preload_tasks._compute_interactive_artifacts(["SPOT"], "2026-04-15")

    assert failed == {}
    assert loaded["SPOT"]["symbol"] == "SPOT"
    assert contexts["SPOT"] is context
    mock_get_context.assert_called_once_with("SPOT")
    mock_build_payload.assert_called_once_with(context, as_of_date="2026-04-15")


def test_interactive_worker_failure_sets_short_backoff_not_background_backoff():
    with patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"):
        preload_tasks._store_worker_results(
            {},
            {"SPOT": "No data found for symbol SPOT"},
            "2026-04-15",
            source="interactive",
        )
        interactive_remaining = preload_tasks._interactive_symbol_backoff_remaining("SPOT", "2026-04-15")
        background_remaining = preload_tasks._symbol_preload_backoff_remaining("SPOT", "2026-04-15")

    assert 0 < interactive_remaining <= preload_tasks.DEFAULT_INTERACTIVE_FAILURE_BACKOFF_SECONDS
    assert background_remaining == 0


def test_successful_store_clears_background_and_interactive_failure_backoffs():
    with patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"):
        preload_tasks._mark_preload_failed("SPOT", as_of_date="2026-04-15")
        preload_tasks._mark_interactive_preload_failed("SPOT", as_of_date="2026-04-15")

        assert preload_tasks._symbol_preload_backoff_remaining("SPOT", "2026-04-15") > 0
        assert preload_tasks._interactive_symbol_backoff_remaining("SPOT", "2026-04-15") > 0

        preload_tasks.store_preloaded_entry_decision(
            "SPOT",
            {"symbol": "SPOT", "horizons": {}, "backtest_1y": {}, "chart_data": []},
            as_of_date="2026-04-15",
        )

        assert preload_tasks._symbol_preload_backoff_remaining("SPOT", "2026-04-15") == 0
        assert preload_tasks._interactive_symbol_backoff_remaining("SPOT", "2026-04-15") == 0


def test_legacy_snapshot_cache_does_not_block_full_model_preload():
    preload_tasks.store_preloaded_entry_decision(
        "AMD",
        {
            "symbol": "AMD",
            "requested_as_of_date": "2026-04-15",
            "as_of_date": "2026-04-15",
            "meta": {"full_decision_preloaded": False},
        },
        as_of_date="2026-04-15",
    )
    daily_scan_tasks._store_cached_result(
        {"timestamp": "2026-04-15 10:05:00", "alerts": [{"symbol": "AMD"}]}
    )

    with (
        patch.dict(os.environ, {"ENTRY_DECISION_PRELOAD_FULL_ENABLED": "1"}),
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        result = preload_tasks.preload_entry_decisions_from_latest_alerts(
            max_symbols=1,
            min_idle_seconds=0,
        )

    assert result == {"status": "started", "symbols": ["AMD"]}
    mock_start.assert_called_once_with(["AMD"], "2026-04-15", source="background")


def test_preload_scheduler_reports_existing_worker_running():
    class FakeProcess:
        def is_alive(self):
            return True

        def terminate(self):
            return None

        def kill(self):
            return None

        def join(self, timeout=None):
            return timeout

    preload_tasks._worker_process = FakeProcess()
    preload_tasks._worker_started_at = time.monotonic()
    preload_tasks._worker_symbols = ["AMD"]
    preload_tasks._worker_as_of_date = "2026-04-15"
    preload_tasks._worker_source = "background"

    result = preload_tasks.preload_entry_decisions_from_latest_alerts(
        max_symbols=1,
        min_idle_seconds=0,
        timeout_seconds=45,
    )

    assert result["status"] == "running"
    assert result["symbols"] == ["AMD"]
    assert result["source"] == "background"


def test_preload_skips_while_request_is_active():
    daily_scan_tasks._store_cached_result(
        {"timestamp": "2026-04-15 10:05:00", "alerts": [{"symbol": "AMD"}]}
    )

    preload_tasks.mark_backend_request_started()
    try:
        with patch("tasks.entry_decision_preload_tasks._compute_preload_artifacts") as mock_compute:
            result = preload_tasks.preload_entry_decisions_from_latest_alerts(
                max_symbols=1,
                min_idle_seconds=0,
            )
    finally:
        preload_tasks.mark_backend_request_finished()

    assert result["status"] == "skipped"
    assert result["reason"] == "backend_busy"
    mock_compute.assert_not_called()
