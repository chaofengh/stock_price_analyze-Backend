from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime
from typing import Iterable

from analysis.data_fetcher_utils import normalize_symbol
from utils.serialization import convert_to_python_types


STORY_VERSION = "entry_signal_story_v1"


def _number(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None


def _date_text(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    return text[:10] if text else None


def _direction_label(direction: str | None) -> str:
    if direction == "continuation":
        return "Continuation"
    if direction == "reversal":
        return "Reversal"
    return "Unknown"


def _direction_word(direction: str | None) -> str:
    if direction == "continuation":
        return "continuation"
    if direction == "reversal":
        return "reversal"
    return "mixed"


def _format_price(value) -> str | None:
    number = _number(value)
    return f"${number:.2f}" if number is not None else None


def _format_percent(value) -> str | None:
    number = _number(value)
    if number is None:
        return None
    return f"{'+' if number >= 0 else ''}{number * 100:.2f}%"


def _format_numeric_range(values, formatter) -> str | None:
    numbers = [number for number in (_number(value) for value in values) if number is not None]
    if not numbers:
        return None
    minimum = min(numbers)
    maximum = max(numbers)
    if abs(minimum - maximum) < 0.000001:
        return formatter(minimum)
    return f"{formatter(minimum)} to {formatter(maximum)}"


def _format_session_count(value: int) -> str:
    return f"{value} session{'s' if value != 1 else ''} left"


def _format_session_range(values) -> str | None:
    numbers = sorted({int(number) for number in (_number(value) for value in values) if number is not None})
    if not numbers:
        return None
    if len(numbers) == 1:
        return _format_session_count(numbers[0])
    return f"{numbers[0]}-{numbers[-1]} sessions left"


def _setup_key(signal: dict) -> str:
    return "|".join(
        [
            str(signal.get("horizon_days") or ""),
            str(signal.get("predicted_direction") or ""),
            str(signal.get("interim_status") or "open"),
        ]
    )


def _signal_sort_key(signal: dict) -> tuple[int, str, int]:
    remaining = _number(signal.get("remaining_sessions"))
    horizon = _number(signal.get("horizon_days"))
    return (
        int(remaining) if remaining is not None else 999,
        _date_text(signal.get("signal_date")) or "",
        int(horizon) if horizon is not None else 999,
    )


def _build_setup(signals: list[dict]) -> dict | None:
    usable = [signal for signal in signals if signal.get("predicted_direction") in {"continuation", "reversal"}]
    if not usable:
        return None

    ordered = sorted(usable, key=_signal_sort_key)
    primary = ordered[0]
    remaining_values = [signal.get("remaining_sessions") for signal in ordered]
    remaining_numbers = [number for number in (_number(value) for value in remaining_values) if number is not None]
    horizon = _number(primary.get("horizon_days"))
    direction = primary.get("predicted_direction")
    interim_status = primary.get("interim_status") or "open"
    signal_dates = [_date_text(signal.get("signal_date")) for signal in ordered]
    signal_dates = [value for value in signal_dates if value]

    return {
        "key": _setup_key(primary),
        "horizon_days": int(horizon) if horizon is not None else None,
        "predicted_direction": direction,
        "direction_label": _direction_label(direction),
        "interim_status": interim_status,
        "signal_count": len(ordered),
        "remaining_min": int(min(remaining_numbers)) if remaining_numbers else None,
        "remaining_max": int(max(remaining_numbers)) if remaining_numbers else None,
        "remaining_summary": _format_session_range(remaining_values),
        "entry_summary": _format_numeric_range(
            [signal.get("signal_close") for signal in ordered],
            _format_price,
        ),
        "return_summary": _format_numeric_range(
            [signal.get("current_trade_return") for signal in ordered],
            _format_percent,
        ),
        "signal_date_start": min(signal_dates) if signal_dates else None,
        "signal_date_end": max(signal_dates) if signal_dates else None,
        "signal_ids": [
            f"entry-signal|{normalize_symbol(signal.get('symbol'))}|{_date_text(signal.get('signal_date'))}|{signal.get('horizon_days')}"
            for signal in ordered
        ],
    }


def _setup_sort_key(setup: dict) -> tuple[int, int, str]:
    horizon = setup.get("horizon_days")
    remaining = setup.get("remaining_min")
    return (
        int(horizon) if horizon is not None else 999,
        int(remaining) if remaining is not None else 999,
        str(setup.get("predicted_direction") or ""),
    )


def _role_setups(setups: list[dict]) -> list[dict]:
    ordered = sorted(setups, key=_setup_sort_key)
    if not ordered:
        return []

    primary_direction = ordered[0].get("predicted_direction")
    output = []
    for index, setup in enumerate(ordered):
        direction = setup.get("predicted_direction")
        if index == 0:
            role = "near_term"
            label = "Near term"
            priority = 0
        elif direction == primary_direction:
            role = "supporting"
            label = "Also supports"
            priority = 1
        else:
            role = "risk"
            label = "Opposing risk"
            priority = 2
        output.append({**setup, "role": role, "label": label, "_role_priority": priority})

    return [
        {key: value for key, value in setup.items() if key != "_role_priority"}
        for setup in sorted(output, key=lambda setup: (setup["_role_priority"], _setup_sort_key(setup)))
    ]


def _setup_phrase(setup: dict) -> str:
    horizon = setup.get("horizon_days")
    return f"{horizon}D {_direction_label(setup.get('predicted_direction'))}" if horizon else _direction_label(setup.get("predicted_direction"))


def _join_phrases(items: list[str]) -> str:
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"
    return f"{', '.join(items[:-1])}, and {items[-1]}"


def _story_text(symbol: str, setups: list[dict]) -> tuple[str, str, str, str]:
    directions = {setup.get("predicted_direction") for setup in setups if setup.get("predicted_direction")}
    phrases = [_setup_phrase(setup) for setup in setups]

    if {"continuation", "reversal"}.issubset(directions):
        primary = setups[0].get("predicted_direction")
        opposing = "reversal" if primary == "continuation" else "continuation"
        headline = (
            f"{symbol} has {_direction_word(primary)} support, but "
            f"{_direction_word(opposing)} risk is still open"
        )
        summary = f"Open setups: {_join_phrases(phrases)}."
        watch = (
            "Use this as competing model evidence, not a single yes/no call. "
            "Compare the near-term setup with the opposing setup before acting."
        )
        return "mixed", headline, summary, watch

    only_direction = next(iter(directions), None)
    if only_direction:
        headline = f"All open {symbol} setups lean {_direction_label(only_direction)}"
        summary = f"Open setups: {_join_phrases(phrases)}."
        watch = "The active model signals point the same way; use details to check timing and open risk."
        return only_direction, headline, summary, watch

    return (
        "unknown",
        f"{symbol} has open model signals",
        f"{len(setups)} setup{'s' if len(setups) != 1 else ''} are open.",
        "Use details to inspect timing and direction.",
    )


def build_open_entry_signal_stories(signals: Iterable[dict] | None) -> list[dict]:
    by_symbol: dict[str, list[dict]] = defaultdict(list)
    for signal in signals or []:
        symbol = normalize_symbol(signal.get("symbol") or signal.get("ticker"))
        if not symbol:
            continue
        by_symbol[symbol].append({**signal, "symbol": symbol})

    stories = []
    for symbol, symbol_signals in sorted(by_symbol.items()):
        setup_groups: dict[str, list[dict]] = defaultdict(list)
        for signal in symbol_signals:
            setup_groups[_setup_key(signal)].append(signal)

        setups = []
        for grouped_signals in setup_groups.values():
            setup = _build_setup(grouped_signals)
            if setup is not None:
                setups.append(setup)
        setups = _role_setups(setups)
        if not setups:
            continue

        stance, headline, summary, watch = _story_text(symbol, setups)
        direction_signal_counts = Counter(
            signal.get("predicted_direction")
            for signal in symbol_signals
            if signal.get("predicted_direction") in {"continuation", "reversal"}
        )
        current_close = next(
            (signal.get("current_close") for signal in symbol_signals if _number(signal.get("current_close")) is not None),
            None,
        )
        remaining_values = [signal.get("remaining_sessions") for signal in symbol_signals]

        stories.append(
            {
                "version": STORY_VERSION,
                "symbol": symbol,
                "stance": stance,
                "headline": headline,
                "summary": summary,
                "watch": watch,
                "signal_count": len(symbol_signals),
                "setup_count": len(setups),
                "direction_signal_counts": dict(direction_signal_counts),
                "current_close": current_close,
                "next_remaining": min(
                    [number for number in (_number(value) for value in remaining_values) if number is not None],
                    default=None,
                ),
                "remaining_summary": _format_session_range(remaining_values),
                "setups": setups,
            }
        )

    return convert_to_python_types(stories)
