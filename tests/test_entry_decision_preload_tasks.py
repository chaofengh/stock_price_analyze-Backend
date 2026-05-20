import os
import time
from datetime import datetime
from unittest.mock import call, patch

import analysis.trade_entry_evaluation as tee
import tasks.daily_scan_tasks as daily_scan_tasks
import tasks.entry_decision_preload_tasks as preload_tasks


def setup_function():
    os.environ["ENTRY_DECISION_PRELOAD_MARKET_HOURS_ONLY"] = "0"
    daily_scan_tasks._reset_scan_state_for_tests()
    preload_tasks._reset_entry_decision_preload_state_for_tests()


def teardown_function():
    daily_scan_tasks._reset_scan_state_for_tests()
    preload_tasks._reset_entry_decision_preload_state_for_tests()
    os.environ.pop("ENTRY_DECISION_PRELOAD_MARKET_HOURS_ONLY", None)


def _entry_context_meta(symbol: str = "AMD", *, price_data_end_date: str = "2099-01-01") -> dict:
    return {
        "symbol": symbol,
        "model_version": tee.entry_decision_model_version(),
        "feature_schema_version": tee.entry_decision_feature_schema_version(),
        "price_data_end_date": price_data_end_date,
        "trained_through_date": price_data_end_date,
        "context_key": f"{symbol}-{price_data_end_date}",
        "quality": {"status": "passed"},
    }


def test_alert_symbols_from_payload_normalizes_and_dedupes():
    payload = {
        "alerts": [
            {"symbol": "amd"},
            {"ticker": "MU"},
            {"symbol": "AMD"},
            "nvda",
            {},
        ],
        "open_entry_signals": [
            {"symbol": "aapl"},
            {"ticker": "MU"},
        ],
    }

    assert preload_tasks.alert_symbols_from_payload(payload) == ["AMD", "MU", "NVDA", "AAPL"]


def test_default_worker_timeout_allows_realistic_cold_entry_model_runtime():
    assert preload_tasks.DEFAULT_WORKER_TIMEOUT_SECONDS >= 180


def test_cache_day_tracks_latest_required_completed_price_date():
    with patch("tasks.entry_decision_preload_tasks.latest_required_price_date", return_value="2026-04-16"):
        assert preload_tasks._cache_day() == "2026-04-16"


def test_preloaded_payload_cache_rotates_when_latest_required_price_date_changes():
    payload = {
        "symbol": "AMD",
        "requested_as_of_date": "2100-01-01",
        "as_of_date": "2100-01-01",
        "meta": {
            "full_decision_preloaded": True,
            "context": _entry_context_meta(price_data_end_date="2100-01-01"),
        },
    }

    with patch("tasks.entry_decision_preload_tasks.latest_required_price_date", return_value="2100-01-01"):
        assert preload_tasks.store_preloaded_entry_decision("AMD", payload, as_of_date="2100-01-01")
        assert preload_tasks.get_preloaded_entry_decision("AMD", as_of_date="2100-01-01") is not None

    with patch("tasks.entry_decision_preload_tasks.latest_required_price_date", return_value="2100-01-02"):
        assert preload_tasks.get_preloaded_entry_decision("AMD", as_of_date="2100-01-01") is None


def test_incomplete_after_close_payload_does_not_stick_in_current_cache_epoch():
    payload = {
        "symbol": "AMD",
        "requested_as_of_date": "2100-01-02",
        "as_of_date": "2100-01-01",
        "meta": {
            "full_decision_preloaded": True,
            "context": _entry_context_meta(price_data_end_date="2100-01-01"),
        },
    }

    with patch("tasks.entry_decision_preload_tasks.latest_required_price_date", return_value="2100-01-02"):
        assert preload_tasks.store_preloaded_entry_decision("AMD", payload, as_of_date="2100-01-02")
        assert preload_tasks.get_preloaded_entry_decision("AMD", as_of_date="2100-01-02") is None


def test_one_session_stale_payload_still_needs_after_close_preload():
    payload = {
        "symbol": "AMD",
        "requested_as_of_date": "2100-01-02",
        "as_of_date": "2100-01-01",
        "meta": {
            "full_decision_preloaded": True,
            "context": _entry_context_meta(price_data_end_date="2100-01-01"),
        },
    }

    with patch("tasks.entry_decision_preload_tasks.latest_required_price_date", return_value="2100-01-02"):
        assert preload_tasks.store_preloaded_entry_decision("AMD", payload, as_of_date="2100-01-02")

        pending = preload_tasks._symbols_needing_preload(
            ["AMD"],
            as_of_date="2100-01-02",
            full_only=True,
        )
        needs_full = preload_tasks._key_needs_full_preload_locked(("2100-01-02", "AMD", "2100-01-02"))

    assert pending == ["AMD"]
    assert needs_full is True


def test_auto_preload_market_gate_blocks_after_chicago_close():
    after_close = preload_tasks._CHICAGO_TZ.localize(datetime(2026, 4, 16, 15, 1))

    with patch.dict(os.environ, {"ENTRY_DECISION_PRELOAD_MARKET_HOURS_ONLY": "1"}):
        result = preload_tasks._auto_preload_market_gate(after_close)

    assert result["allowed"] is False
    assert result["reason"] == "market_closed"
    assert result["market_close_at"] == "2026-04-16 15:00:00"


def test_auto_preload_market_gate_blocks_new_work_near_close():
    near_close = preload_tasks._CHICAGO_TZ.localize(datetime(2026, 4, 16, 14, 56))

    with patch.dict(
        os.environ,
        {
            "ENTRY_DECISION_PRELOAD_MARKET_HOURS_ONLY": "1",
            "ENTRY_DECISION_PRELOAD_CLOSE_CUTOFF_MINUTES": "5",
        },
    ):
        result = preload_tasks._auto_preload_market_gate(near_close)

    assert result["allowed"] is False
    assert result["reason"] == "market_close_cutoff"
    assert result["preload_cutoff_at"] == "2026-04-16 14:55:00"


def test_background_preload_skips_after_market_close_without_starting_work():
    daily_scan_tasks._store_cached_result(
        {"timestamp": "2026-04-15 10:05:00", "alerts": [{"symbol": "AMD"}]}
    )

    with (
        patch.dict(os.environ, {"ENTRY_DECISION_PRELOAD_MARKET_HOURS_ONLY": "1"}),
        patch(
            "tasks.entry_decision_preload_tasks._auto_preload_market_gate",
            return_value={
                "allowed": False,
                "reason": "market_closed",
                "next_run_at": "2026-04-16 08:35:00",
            },
        ),
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
        patch("tasks.entry_decision_preload_tasks._compute_preload_artifacts") as mock_compute,
    ):
        result = preload_tasks.preload_entry_decisions_from_latest_alerts(
            max_symbols=1,
            min_idle_seconds=0,
        )

    assert result == {
        "status": "skipped",
        "reason": "market_closed",
        "next_run_at": "2026-04-16 08:35:00",
    }
    mock_start.assert_not_called()
    mock_compute.assert_not_called()


def test_alert_payload_preload_skips_after_market_close_when_market_gate_is_requested():
    payload = {"timestamp": "2026-04-15 10:05:00", "alerts": [{"symbol": "UBER"}]}

    with (
        patch.dict(os.environ, {"ENTRY_DECISION_PRELOAD_MARKET_HOURS_ONLY": "1"}),
        patch(
            "tasks.entry_decision_preload_tasks._auto_preload_market_gate",
            return_value={
                "allowed": False,
                "reason": "market_closed",
                "next_run_at": "2026-04-16 08:35:00",
            },
        ),
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        result = preload_tasks.preload_entry_decisions_from_alert_payload(
            payload,
            min_idle_seconds=0,
            respect_market_gate=True,
        )

    assert result == {
        "status": "skipped",
        "reason": "market_closed",
        "next_run_at": "2026-04-16 08:35:00",
    }
    assert preload_tasks._alert_preload_queue == []
    assert preload_tasks._alert_preload_queued == set()
    mock_start.assert_not_called()


def test_user_alert_payload_preload_bypasses_market_gate_by_default():
    payload = {"timestamp": "2026-04-15 16:35:00", "alerts": [{"symbol": "UBER"}]}

    with (
        patch.dict(os.environ, {"ENTRY_DECISION_PRELOAD_FULL_ENABLED": "1"}),
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks._auto_preload_market_gate") as mock_gate,
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        result = preload_tasks.preload_entry_decisions_from_alert_payload(payload, min_idle_seconds=0)

    assert result["status"] == "started"
    assert result["symbols"] == ["UBER"]
    mock_gate.assert_not_called()
    mock_start.assert_called_once_with(["UBER"], "2026-04-15", source="alert")


def test_market_close_gate_terminates_running_noninteractive_worker():
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
    preload_tasks._worker_symbols = ["AMD"]
    preload_tasks._worker_as_of_date = "2026-04-15"
    preload_tasks._worker_source = "alert"

    with (
        patch.dict(os.environ, {"ENTRY_DECISION_PRELOAD_MARKET_HOURS_ONLY": "1"}),
        patch(
            "tasks.entry_decision_preload_tasks._auto_preload_market_gate",
            return_value={
                "allowed": False,
                "reason": "market_close_cutoff",
                "next_run_at": "2026-04-16 08:35:00",
            },
        ),
    ):
        result = preload_tasks.preload_entry_decisions_from_latest_alerts(
            max_symbols=1,
            min_idle_seconds=0,
        )

    assert result["status"] == "skipped"
    assert result["reason"] == "market_close_cutoff"
    assert result["terminated_worker"] == {
        "status": "terminated",
        "reason": "market_close_cutoff",
        "source": "alert",
        "symbols": ["AMD"],
        "as_of_date": "2026-04-15",
    }
    assert not active_process.is_alive()
    assert preload_tasks._worker_process is None


def test_market_close_gate_does_not_terminate_interactive_worker():
    class FakeProcess:
        terminated = False
        killed = False

        def is_alive(self):
            return True

        def terminate(self):
            self.terminated = True

        def kill(self):
            self.killed = True

        def join(self, timeout=None):
            return timeout

    active_process = FakeProcess()
    preload_tasks._worker_process = active_process
    preload_tasks._worker_started_at = time.monotonic()
    preload_tasks._worker_symbols = ["AMD"]
    preload_tasks._worker_as_of_date = "2026-04-15"
    preload_tasks._worker_source = "interactive"

    with (
        patch.dict(os.environ, {"ENTRY_DECISION_PRELOAD_MARKET_HOURS_ONLY": "1"}),
        patch(
            "tasks.entry_decision_preload_tasks._auto_preload_market_gate",
            return_value={
                "allowed": False,
                "reason": "market_closed",
                "next_run_at": "2026-04-16 08:35:00",
            },
        ),
    ):
        result = preload_tasks.preload_entry_decisions_from_latest_alerts(
            max_symbols=1,
            min_idle_seconds=0,
        )

    assert result["status"] == "skipped"
    assert result["reason"] == "market_closed"
    assert result["worker"]["status"] == "running"
    assert result["worker"]["source"] == "interactive"
    assert preload_tasks._worker_process is active_process
    assert active_process.terminated is False
    assert active_process.killed is False
    preload_tasks._clear_worker_state()


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
                "meta": {
                    "full_decision_preloaded": True,
                    "context": _entry_context_meta(symbols[0], price_data_end_date="2099-01-01"),
                },
            }
        }
        contexts = {
            symbols[0]: {
                "symbol": symbols[0],
                "feature_df": object(),
                "decisions_by_index": {},
                "backtest_1y": {},
                "meta": _entry_context_meta(symbols[0], price_data_end_date="2099-01-01"),
            }
        }
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
    assert mock_build_context.call_args.kwargs["earnings_dates"] is None
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
        patch("tasks.entry_decision_preload_tasks.get_open_entry_signal_symbols", return_value=[]),
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


def test_after_close_preload_discovers_open_signals_across_tracked_tickers():
    payload = {"timestamp": "2026-04-15 15:35:00", "alerts": [{"symbol": "AMD"}]}

    with (
        patch.dict(os.environ, {"ENTRY_DECISION_PRELOAD_FULL_ENABLED": "1"}),
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks.get_open_entry_signal_symbols", return_value=["AAPL"]),
        patch("database.ticker_repository.get_all_tickers", return_value=["MSFT", "AMD"]),
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        result = preload_tasks._preload_entry_decisions_from_alert_payload_locked(
            payload,
            max_symbols=-1,
            source="after_close",
            as_of_date="2026-04-15",
        )

    assert result == {"status": "started", "symbols": ["AMD", "AAPL", "MSFT"]}
    mock_start.assert_called_once_with(["AMD", "AAPL", "MSFT"], "2026-04-15", source="after_close")


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


def test_after_close_preload_bypasses_market_gate_and_warms_all_alert_symbols():
    daily_scan_tasks._store_cached_result(
        {
            "timestamp": "2026-04-15 15:10:00",
            "alerts": [{"symbol": "AMD"}, {"symbol": "MU"}, {"symbol": "AAPL"}],
        }
    )

    with (
        patch.dict(
            os.environ,
            {
                "ENTRY_DECISION_PRELOAD_FULL_ENABLED": "1",
                "ENTRY_DECISION_PRELOAD_MARKET_HOURS_ONLY": "1",
            },
        ),
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks._auto_preload_market_gate") as mock_gate,
        patch("tasks.entry_decision_preload_tasks.get_open_entry_signal_symbols", return_value=[]),
        patch("database.ticker_repository.get_all_tickers", return_value=[]),
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        result = preload_tasks.refresh_entry_decisions_for_latest_alerts_after_close()

    assert result == {"status": "started", "symbols": ["AMD", "MU", "AAPL"]}
    mock_gate.assert_not_called()
    mock_start.assert_called_once_with(["AMD", "MU", "AAPL"], "2026-04-15", source="after_close")


def test_startup_preload_bypasses_market_gate_and_warms_all_alert_symbols():
    daily_scan_tasks._store_cached_result(
        {
            "timestamp": "2026-04-15 07:00:00",
            "alerts": [{"symbol": "AMD"}, {"symbol": "MU"}],
        }
    )

    with (
        patch.dict(
            os.environ,
            {
                "ENTRY_DECISION_PRELOAD_FULL_ENABLED": "1",
                "ENTRY_DECISION_PRELOAD_MARKET_HOURS_ONLY": "1",
            },
        ),
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks._auto_preload_market_gate") as mock_gate,
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        result = preload_tasks.preload_entry_decisions_for_startup_alerts()

    assert result == {"status": "started", "symbols": ["AMD", "MU"]}
    mock_gate.assert_not_called()
    mock_start.assert_called_once_with(["AMD", "MU"], "2026-04-15", source="startup")


def test_resource_capped_preload_artifacts_chunks_work_and_pauses_between_batches():
    def fake_compute(symbols, as_of_date):
        return (
            {symbol: {"symbol": symbol} for symbol in symbols},
            {symbol: {"context": symbol} for symbol in symbols},
            {},
        )

    with (
        patch.dict(
            os.environ,
            {
                "ENTRY_DECISION_RESOURCE_CAPPED_PRELOAD_BATCH_SIZE": "2",
                "ENTRY_DECISION_RESOURCE_CAPPED_PRELOAD_PAUSE_SECONDS": "0.01",
            },
        ),
        patch(
            "tasks.entry_decision_preload_tasks._compute_preload_artifacts",
            side_effect=fake_compute,
        ) as mock_compute,
        patch("tasks.entry_decision_preload_tasks.time.sleep") as mock_sleep,
    ):
        loaded, contexts, failed = preload_tasks._compute_resource_capped_preload_artifacts(
            ["AMD", "MU", "AAPL", "NVDA", "MSFT"],
            "2026-04-15",
        )

    assert failed == {}
    assert list(loaded) == ["AMD", "MU", "AAPL", "NVDA", "MSFT"]
    assert list(contexts) == ["AMD", "MU", "AAPL", "NVDA", "MSFT"]
    assert mock_compute.call_args_list == [
        call(["AMD", "MU"], "2026-04-15"),
        call(["AAPL", "NVDA"], "2026-04-15"),
        call(["MSFT"], "2026-04-15"),
    ]
    assert mock_sleep.call_args_list == [call(0.01), call(0.01)]


def test_startup_and_after_close_workers_lower_process_priority():
    with (
        patch.dict(os.environ, {"ENTRY_DECISION_BACKGROUND_PRELOAD_NICE": "7"}),
        patch("tasks.entry_decision_preload_tasks.os.nice") as mock_nice,
    ):
        preload_tasks._apply_preload_worker_resource_limits("startup")
        preload_tasks._apply_preload_worker_resource_limits("after_close")
        preload_tasks._apply_preload_worker_resource_limits("background")

    assert mock_nice.call_args_list == [call(7), call(7)]


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
    context = {
        "symbol": "UBER",
        "feature_df": object(),
        "decisions_by_index": {},
        "backtest_1y": {},
        "meta": _entry_context_meta("UBER", price_data_end_date="2099-01-01"),
    }

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
                "meta": {
                    "full_decision_preloaded": True,
                    "context": _context["meta"],
                },
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
    context = {
        "symbol": "UBER",
        "feature_df": object(),
        "decisions_by_index": {},
        "backtest_1y": {},
        "meta": _entry_context_meta("UBER", price_data_end_date="2099-01-01"),
    }

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
                "meta": {
                    "full_decision_preloaded": True,
                    "context": _context["meta"],
                },
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


def test_user_alert_payload_ready_cache_resyncs_entry_signals():
    alert_payload = {"timestamp": "2026-05-20 13:21:43", "open_entry_signals": [{"symbol": "AAPL"}]}
    cached_payload = {
        "symbol": "AAPL",
        "as_of_date": "2026-05-20",
        "horizons": {},
        "backtest_1y": {
            "10d": {
                "open_predictions": [
                    {"signal_date": "2026-05-06", "horizon_days": 10, "predicted_direction": "long"},
                    {"signal_date": "2026-05-07", "horizon_days": 10, "predicted_direction": "long"},
                    {"signal_date": "2026-05-08", "horizon_days": 10, "predicted_direction": "short"},
                    {"signal_date": "2026-05-11", "horizon_days": 10, "predicted_direction": "short"},
                ],
                "predictions": [],
            }
        },
        "chart_data": [],
        "meta": {
            "full_decision_preloaded": True,
            "context": _entry_context_meta("AAPL", price_data_end_date="2026-05-20"),
        },
    }

    sync_result = {"status": "synced", "symbol": "AAPL", "open": 4, "closed": 1}
    with (
        patch.dict(os.environ, {"ENTRY_DECISION_PRELOAD_FULL_ENABLED": "1"}),
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-05-20"),
        patch("tasks.entry_decision_preload_tasks.safe_sync_entry_signals_from_payload", return_value=sync_result) as mock_sync,
    ):
        preload_tasks.store_preloaded_entry_decision("AAPL", cached_payload, as_of_date="2026-05-20")
        result = preload_tasks.preload_entry_decisions_from_alert_payload(alert_payload, min_idle_seconds=0)

    assert result == {
        "status": "ready",
        "loaded": 0,
        "symbols": ["AAPL"],
        "entry_signal_sync": {"AAPL": sync_result},
    }
    assert mock_sync.call_count == 1
    sync_args, sync_kwargs = mock_sync.call_args
    assert sync_args[0] == "AAPL"
    assert sync_args[1]["as_of_date"] == "2026-05-20"
    assert sync_kwargs == {"source": "alert"}


def test_preloaded_context_renders_selected_date_without_new_worker_payload():
    context = {
        "symbol": "AMD",
        "feature_df": object(),
        "decisions_by_index": {},
        "backtest_1y": {},
        "meta": _entry_context_meta("AMD", price_data_end_date="2099-01-01"),
    }
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


def test_preloaded_fresh_context_renders_selected_date_without_new_worker_payload():
    context = {
        "symbol": "AMD",
        "feature_df": object(),
        "decisions_by_index": {},
        "backtest_1y": {},
        "meta": _entry_context_meta("AMD"),
    }

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
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        assert preload_tasks.store_entry_decision_context("AMD", context)
        payload = preload_tasks.get_preloaded_entry_decision("AMD", as_of_date="2026-04-10", full_only=True)

    assert payload["requested_as_of_date"] == "2026-04-10"
    mock_build_payload.assert_called_once_with(context, as_of_date="2026-04-10")
    mock_start.assert_not_called()


def test_expired_context_cache_is_not_used_for_selected_date():
    context = {
        "symbol": "AMD",
        "feature_df": object(),
        "decisions_by_index": {},
        "backtest_1y": {},
        "meta": _entry_context_meta("AMD", price_data_end_date="2024-01-02"),
    }

    with (
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks.build_entry_decision_from_context") as mock_build_payload,
    ):
        assert preload_tasks.store_entry_decision_context("AMD", context)
        payload = preload_tasks.get_preloaded_entry_decision("AMD", as_of_date="2026-04-10", full_only=True)

    assert payload is None
    mock_build_payload.assert_not_called()


def test_expired_full_payload_cache_is_evicted_and_requeued_for_preload():
    stale_payload = {
        "symbol": "AMD",
        "requested_as_of_date": "2026-04-15",
        "as_of_date": "2026-04-15",
        "horizons": {},
        "backtest_1y": {},
        "chart_data": [],
        "meta": {
            "full_decision_preloaded": True,
            "context": _entry_context_meta("AMD", price_data_end_date="2024-01-02"),
        },
    }

    daily_scan_tasks._store_cached_result(
        {
            "timestamp": "2026-04-15 10:05:00",
            "alerts": [{"symbol": "AMD"}],
        }
    )

    with (
        patch.dict(os.environ, {"ENTRY_DECISION_PRELOAD_FULL_ENABLED": "1"}),
        patch("tasks.entry_decision_preload_tasks._cache_day", return_value="2026-04-15"),
        patch("tasks.entry_decision_preload_tasks._start_preload_worker") as mock_start,
    ):
        assert preload_tasks.store_preloaded_entry_decision("AMD", stale_payload, as_of_date="2026-04-15")
        assert preload_tasks.get_preloaded_entry_decision("AMD", as_of_date="2026-04-15", full_only=True) is None

        result = preload_tasks.preload_entry_decisions_from_latest_alerts(
            max_symbols=1,
            min_idle_seconds=0,
        )

    assert result == {"status": "started", "symbols": ["AMD"]}
    mock_start.assert_called_once_with(["AMD"], "2026-04-15", source="background")


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
