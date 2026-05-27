from datetime import date

from database import entry_signal_repository as repository
from database.entry_signal_repository import _normalize_output_direction


def test_normalize_output_direction_maps_legacy_upper_continuation_to_long():
    row = {
        "symbol": "AAPL",
        "touched_side": "Upper",
        "predicted_direction": "continuation",
        "trade_direction": None,
    }

    normalized = _normalize_output_direction(row)

    assert normalized["predicted_direction"] == "long"
    assert normalized["trade_direction"] == "long"


def test_normalize_output_direction_maps_legacy_lower_continuation_to_short():
    row = {
        "symbol": "AAPL",
        "touched_side": "Lower",
        "predicted_direction": "continuation",
        "trade_direction": None,
    }

    normalized = _normalize_output_direction(row)

    assert normalized["predicted_direction"] == "short"
    assert normalized["trade_direction"] == "short"


def test_list_open_entry_signals_serializes_dates_without_running_schema_ddl(monkeypatch):
    executed = []

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params=None):
            executed.append(str(query))

        def fetchall(self):
            return [
                {
                    "symbol": "AAPL",
                    "signal_date": date(2026, 5, 6),
                    "horizon_days": 10,
                    "status": "open",
                    "touched_side": "Upper",
                    "predicted_direction": "long",
                    "trade_direction": "long",
                    "signal_close": 287.245361,
                    "prediction_end_date": date(2026, 5, 20),
                    "current_date": date(2026, 5, 19),
                    "current_close": 298.970001,
                    "elapsed_sessions": 9,
                    "remaining_sessions": 1,
                    "progress": 0.9,
                    "interim_direction": "long",
                    "interim_status": "working",
                    "current_trade_return": 0.040818,
                    "current_trade_return_atr": 1.751059,
                    "continuation_probability": 0.815003,
                    "reversal_probability": 0.184997,
                    "confidence_score": 82,
                    "signal_model": "Selective Coverage Repair",
                    "signal_model_id": "coverage_repair",
                    "signal_precision": 0.857143,
                    "signal_tier": "coverage_repair",
                    "source": "entry_decision_api",
                    "model_version": "entry-test",
                    "feature_schema_version": "features-test",
                    "payload_as_of_date": date(2026, 5, 19),
                    "price_data_end_date": date(2026, 5, 19),
                    "key_reasons": None,
                    "playbook": None,
                    "updated_at": date(2026, 5, 20),
                }
            ]

    class FakeConnection:
        def cursor(self, *args, **kwargs):
            return FakeCursor()

        def commit(self):
            pass

        def close(self):
            pass

    monkeypatch.setattr(repository, "get_connection", lambda: FakeConnection())

    rows = repository.list_open_entry_signals(symbols=["AAPL"])

    assert rows[0]["signal_date"] == "2026-05-06"
    assert rows[0]["prediction_end_date"] == "2026-05-20"
    assert rows[0]["current_date"] == "2026-05-19"
    assert not any("CREATE " in query or "ALTER TABLE" in query for query in executed)


def test_close_open_entry_signals_absent_from_keys_closes_only_missing_current_horizon_rows(monkeypatch):
    executed = []

    class FakeCursor:
        rowcount = 1

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params=None):
            executed.append((str(query), params))

    class FakeConnection:
        def cursor(self, *args, **kwargs):
            return FakeCursor()

        def commit(self):
            pass

        def rollback(self):
            pass

        def close(self):
            pass

    monkeypatch.setattr(repository, "get_connection", lambda: FakeConnection())

    count = repository.close_open_entry_signals_absent_from_keys(
        "AAPL",
        open_keys={
            ("2026-05-06", 10),
            ("2026-05-07", 10),
            ("2026-05-08", 10),
            ("2026-05-11", 10),
        },
        horizon_days={10},
        current_date="2026-05-20",
    )

    query, params = executed[0]
    assert count == 1
    assert "UPDATE entry_decision_signals" in query
    assert "status = 'closed'" in query
    assert "horizon_days = ANY(%s)" in query
    assert "2026-05-13" not in params
    assert params[:4] == ["2026-05-20", "2026-05-20", "AAPL", [10]]
    assert params[4:] == [
        "2026-05-06",
        10,
        "2026-05-07",
        10,
        "2026-05-08",
        10,
        "2026-05-11",
        10,
    ]
    assert not any("CREATE " in item[0] or "ALTER TABLE" in item[0] for item in executed)
