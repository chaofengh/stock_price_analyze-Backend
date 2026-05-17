from tasks.entry_signal_story_tasks import build_open_entry_signal_stories


def _signal(
    *,
    signal_date,
    horizon_days,
    predicted_direction,
    interim_status,
    remaining_sessions,
    signal_close,
    current_trade_return,
):
    return {
        "symbol": "aapl",
        "signal_date": signal_date,
        "horizon_days": horizon_days,
        "predicted_direction": predicted_direction,
        "interim_status": interim_status,
        "remaining_sessions": remaining_sessions,
        "signal_close": signal_close,
        "current_close": 300.23,
        "current_trade_return": current_trade_return,
    }


def test_open_entry_signal_story_keeps_every_active_setup_visible():
    stories = build_open_entry_signal_stories(
        [
            _signal(
                signal_date="2026-05-11",
                horizon_days=5,
                predicted_direction="continuation",
                interim_status="working",
                remaining_sessions=1,
                signal_close=292.68,
                current_trade_return=0.0258,
            ),
            _signal(
                signal_date="2026-05-04",
                horizon_days=10,
                predicted_direction="continuation",
                interim_status="working",
                remaining_sessions=1,
                signal_close=276.58,
                current_trade_return=0.0855,
            ),
            _signal(
                signal_date="2026-05-06",
                horizon_days=10,
                predicted_direction="continuation",
                interim_status="working",
                remaining_sessions=3,
                signal_close=287.25,
                current_trade_return=0.0452,
            ),
            _signal(
                signal_date="2026-05-07",
                horizon_days=10,
                predicted_direction="continuation",
                interim_status="working",
                remaining_sessions=4,
                signal_close=287.18,
                current_trade_return=0.0455,
            ),
            _signal(
                signal_date="2026-05-08",
                horizon_days=10,
                predicted_direction="reversal",
                interim_status="against",
                remaining_sessions=5,
                signal_close=293.05,
                current_trade_return=-0.0245,
            ),
            _signal(
                signal_date="2026-05-11",
                horizon_days=10,
                predicted_direction="reversal",
                interim_status="against",
                remaining_sessions=6,
                signal_close=292.68,
                current_trade_return=-0.0258,
            ),
        ]
    )

    assert len(stories) == 1
    story = stories[0]
    assert story["symbol"] == "AAPL"
    assert story["stance"] == "mixed"
    assert story["headline"] == "AAPL has continuation support, but reversal risk is still open"
    assert story["summary"] == "Open setups: 5D Continuation, 10D Continuation, and 10D Reversal."
    assert story["direction_signal_counts"] == {"continuation": 4, "reversal": 2}
    assert story["remaining_summary"] == "1-6 sessions left"

    setups = story["setups"]
    assert [(setup["label"], setup["role"]) for setup in setups] == [
        ("Near term", "near_term"),
        ("Also supports", "supporting"),
        ("Opposing risk", "risk"),
    ]
    assert [(setup["horizon_days"], setup["predicted_direction"]) for setup in setups] == [
        (5, "continuation"),
        (10, "continuation"),
        (10, "reversal"),
    ]

    ten_day_continue = setups[1]
    assert ten_day_continue["signal_count"] == 3
    assert ten_day_continue["remaining_summary"] == "1-4 sessions left"
    assert ten_day_continue["entry_summary"] == "$276.58 to $287.25"
    assert ten_day_continue["return_summary"] == "+4.52% to +8.55%"

    ten_day_reversal = setups[2]
    assert ten_day_reversal["signal_count"] == 2
    assert ten_day_reversal["remaining_summary"] == "5-6 sessions left"
    assert ten_day_reversal["entry_summary"] == "$292.68 to $293.05"
    assert ten_day_reversal["return_summary"] == "-2.58% to -2.45%"
