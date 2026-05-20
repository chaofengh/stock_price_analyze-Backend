import tasks.entry_signal_tasks as signal_tasks


def _payload(*, as_of_date="2026-05-15", price_data_end_date="2026-05-15", open_predictions=None, predictions=None):
    return {
        "symbol": "AAPL",
        "as_of_date": as_of_date,
        "meta": {
            "context": {
                "model_version": "entry-test",
                "feature_schema_version": "features-test",
                "price_data_end_date": price_data_end_date,
            }
        },
        "horizons": {
            "5d": {
                "playbook": {"id": "test-playbook", "name": "Empirical Regime Tape", "precision": 0.83},
                "key_reasons": [{"feature": "Empirical Regime Tape", "value": 6}],
            }
        },
        "backtest_1y": {
            "5d": {
                "open_predictions": open_predictions or [],
                "predictions": predictions or [],
            }
        },
    }


def test_open_entry_signal_rows_from_current_payload_carry_model_context():
    payload = _payload(
        open_predictions=[
            {
                "status": "open",
                "signal_date": "2026-05-15",
                "current_date": "2026-05-15",
                "horizon_days": 5,
                "elapsed_sessions": 0,
                "remaining_sessions": 5,
                "progress": 0.0,
                "touched_side": "Upper",
                "predicted_direction": "continuation",
                "trade_direction": "long",
                "signal_close": 100.0,
                "prediction_end_date": "2026-05-22",
                "current_close": 100.0,
                "current_trade_return": 0.0,
                "confidence_score": 75,
                "signal_model": "Empirical Regime Tape",
                "signal_model_id": "empirical_recent_side_only_6",
                "signal_precision": 0.833333,
                "signal_tier": "regime",
                "price_window": [
                    {"date": "2026-05-14", "open": 99.0, "high": 101.0, "low": 98.0, "close": 100.0},
                    {"date": "2026-05-15", "open": 100.0, "high": 102.0, "low": 99.5, "close": 101.0},
                ],
            }
        ]
    )

    rows = signal_tasks.open_entry_signal_rows_from_payload("aapl", payload, source="alert")

    assert len(rows) == 1
    row = rows[0]
    assert row["symbol"] == "AAPL"
    assert row["status"] == "open"
    assert row["signal_date"] == "2026-05-15"
    assert row["horizon_days"] == 5
    assert row["predicted_direction"] == "long"
    assert row["prediction_end_date"] == "2026-05-22"
    assert row["model_version"] == "entry-test"
    assert row["feature_schema_version"] == "features-test"
    assert row["price_data_end_date"] == "2026-05-15"
    assert row["key_reasons"] == [{"feature": "Empirical Regime Tape", "value": 6}]
    assert row["playbook"]["id"] == "test-playbook"
    assert row["price_window"][0]["date"] == "2026-05-14"


def test_sync_entry_signals_closes_existing_open_signal_when_outcome_is_available(monkeypatch):
    upserted = []
    monkeypatch.setattr(signal_tasks, "list_open_entry_signal_keys", lambda symbol: {("2026-05-11", 5)})
    monkeypatch.setattr(signal_tasks, "upsert_entry_decision_signal", lambda row: upserted.append(row))
    monkeypatch.setattr(signal_tasks, "close_open_entry_signals_absent_from_keys", lambda *args, **kwargs: 0)
    payload = _payload(
        predictions=[
            {
                "signal_date": "2026-05-11",
                "outcome_date": "2026-05-18",
                "horizon_days": 5,
                "touched_side": "Upper",
                "predicted_direction": "continuation",
                "actual_direction": "continuation",
                "is_correct": True,
                "signal_close": 292.68,
                "outcome_close": 300.23,
                "trade_direction": "long",
                "trade_return": 0.025796,
                "trade_return_atr": 1.158716,
                "confidence_score": 75,
                "signal_model": "Empirical Regime Tape",
                "signal_model_id": "empirical_recent_side_only_6",
                "signal_precision": 0.833333,
                "signal_tier": "regime",
            }
        ]
    )

    result = signal_tasks.sync_entry_signals_from_payload("AAPL", payload, source="after_close")

    assert result["status"] == "synced"
    assert result["open"] == 0
    assert result["closed"] == 1
    assert len(upserted) == 1
    assert upserted[0]["status"] == "closed"
    assert upserted[0]["signal_date"] == "2026-05-11"
    assert upserted[0]["outcome_date"] == "2026-05-18"
    assert upserted[0]["is_correct"] is True


def test_sync_entry_signals_skips_historical_date_picker_payload(monkeypatch):
    upserted = []
    monkeypatch.setattr(signal_tasks, "list_open_entry_signal_keys", lambda symbol: set())
    monkeypatch.setattr(signal_tasks, "upsert_entry_decision_signal", lambda row: upserted.append(row))
    payload = _payload(
        as_of_date="2026-05-11",
        price_data_end_date="2026-05-15",
        open_predictions=[
            {
                "signal_date": "2026-05-11",
                "current_date": "2026-05-11",
                "horizon_days": 5,
                "touched_side": "Upper",
                "predicted_direction": "continuation",
            }
        ],
    )

    result = signal_tasks.sync_entry_signals_from_payload("AAPL", payload)

    assert result == {"status": "skipped", "reason": "historical_payload"}
    assert upserted == []


def test_sync_entry_signals_closes_open_rows_absent_from_current_model_payload(monkeypatch):
    upserted = []
    stale_close_calls = []
    monkeypatch.setattr(
        signal_tasks,
        "list_open_entry_signal_keys",
        lambda symbol: {
            ("2026-05-06", 10),
            ("2026-05-07", 10),
            ("2026-05-08", 10),
            ("2026-05-11", 10),
            ("2026-05-13", 10),
        },
    )
    monkeypatch.setattr(signal_tasks, "upsert_entry_decision_signal", lambda row: upserted.append(row))

    def close_absent(symbol, **kwargs):
        stale_close_calls.append({"symbol": symbol, **kwargs})
        return 1

    monkeypatch.setattr(signal_tasks, "close_open_entry_signals_absent_from_keys", close_absent)
    payload = _payload(
        as_of_date="2026-05-20",
        price_data_end_date="2026-05-20",
        open_predictions=[
            {
                "signal_date": "2026-05-06",
                "current_date": "2026-05-20",
                "horizon_days": 10,
                "touched_side": "Upper",
                "predicted_direction": "continuation",
                "trade_direction": "long",
            },
            {
                "signal_date": "2026-05-07",
                "current_date": "2026-05-20",
                "horizon_days": 10,
                "touched_side": "Upper",
                "predicted_direction": "continuation",
                "trade_direction": "long",
            },
            {
                "signal_date": "2026-05-08",
                "current_date": "2026-05-20",
                "horizon_days": 10,
                "touched_side": "Upper",
                "predicted_direction": "reversal",
                "trade_direction": "short",
            },
            {
                "signal_date": "2026-05-11",
                "current_date": "2026-05-20",
                "horizon_days": 10,
                "touched_side": "Upper",
                "predicted_direction": "reversal",
                "trade_direction": "short",
            },
        ],
    )
    payload["backtest_1y"] = {"10d": payload["backtest_1y"]["5d"]}

    result = signal_tasks.sync_entry_signals_from_payload("AAPL", payload, source="entry_decision_api")

    assert result["status"] == "synced"
    assert result["open"] == 4
    assert result["closed"] == 1
    assert len(upserted) == 4
    assert stale_close_calls == [
        {
            "symbol": "AAPL",
            "open_keys": {
                ("2026-05-06", 10),
                ("2026-05-07", 10),
                ("2026-05-08", 10),
                ("2026-05-11", 10),
            },
            "horizon_days": {10},
            "current_date": "2026-05-20",
        }
    ]
