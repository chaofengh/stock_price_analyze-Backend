from __future__ import annotations

from datetime import date, datetime
from threading import Lock
from typing import Iterable

from psycopg2.extras import Json, RealDictCursor

from .connection import get_connection
from .create_entry_decision_signals_table import ENTRY_DECISION_SIGNALS_SCHEMA_SQL
from utils.serialization import convert_to_python_types


_UPSERT_FIELDS = [
    "symbol",
    "signal_date",
    "horizon_days",
    "status",
    "touched_side",
    "predicted_direction",
    "trade_direction",
    "signal_close",
    "prediction_end_date",
    "current_date",
    "current_close",
    "outcome_date",
    "outcome_close",
    "elapsed_sessions",
    "remaining_sessions",
    "progress",
    "interim_direction",
    "interim_status",
    "actual_direction",
    "is_correct",
    "current_trade_return",
    "current_trade_return_atr",
    "trade_return",
    "trade_return_atr",
    "continuation_probability",
    "reversal_probability",
    "confidence_score",
    "signal_model",
    "signal_model_id",
    "signal_precision",
    "signal_tier",
    "source",
    "model_version",
    "feature_schema_version",
    "payload_as_of_date",
    "price_data_end_date",
    "key_reasons",
    "playbook",
    "price_window",
]

_DB_COLUMN_BY_FIELD = {
    "current_date": "current_price_date",
}

_JSON_COLUMNS = {"key_reasons", "playbook", "price_window"}
_SCHEMA_LOCK = Lock()
_SCHEMA_READY = False
_SCHEMA_ADVISORY_LOCK_KEY = "entry_decision_signals_schema_v2"


def _normalize_symbol(symbol) -> str | None:
    if symbol is None:
        return None
    normalized = str(symbol).strip().upper()
    return normalized or None


def _normalize_symbols(symbols: Iterable[str] | None) -> list[str]:
    out: list[str] = []
    seen = set()
    for symbol in symbols or []:
        normalized = _normalize_symbol(symbol)
        if not normalized or normalized in seen:
            continue
        out.append(normalized)
        seen.add(normalized)
    return out


def _date_text(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        return None
    return text[:10]


def _json_param(value):
    if value is None:
        return None
    return Json(convert_to_python_types(value))


def _trade_direction_from_model_direction(touched_side: str | None, direction: str | None) -> str | None:
    if direction in ("long", "short"):
        return direction
    if direction not in ("continuation", "reversal"):
        return None
    if touched_side == "Upper":
        return "long" if direction == "continuation" else "short"
    if touched_side == "Lower":
        return "short" if direction == "continuation" else "long"
    return None


def _normalize_output_direction(row: dict) -> dict:
    predicted_direction = row.get("predicted_direction")
    trade_direction = row.get("trade_direction")
    output_direction = (
        trade_direction
        if trade_direction in ("long", "short")
        else _trade_direction_from_model_direction(row.get("touched_side"), predicted_direction)
    )
    if output_direction not in ("long", "short"):
        return row
    return {
        **row,
        "predicted_direction": output_direction,
        "trade_direction": output_direction,
    }


def _ensure_table(cur) -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    with _SCHEMA_LOCK:
        if _SCHEMA_READY:
            return
        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s));", (_SCHEMA_ADVISORY_LOCK_KEY,))
        for statement in ENTRY_DECISION_SIGNALS_SCHEMA_SQL:
            cur.execute(statement)
        _SCHEMA_READY = True


def _row_to_params(row: dict) -> list:
    params = []
    for column in _UPSERT_FIELDS:
        value = row.get(column)
        if column == "symbol":
            value = _normalize_symbol(value)
        elif column in {
            "signal_date",
            "prediction_end_date",
            "current_date",
            "outcome_date",
            "payload_as_of_date",
            "price_data_end_date",
        }:
            value = _date_text(value)
        elif column in _JSON_COLUMNS:
            value = _json_param(value)
        params.append(value)
    return params


def _db_column(field: str) -> str:
    return _DB_COLUMN_BY_FIELD.get(field, field)


def upsert_entry_decision_signal(row: dict) -> None:
    symbol = _normalize_symbol(row.get("symbol"))
    signal_date = _date_text(row.get("signal_date"))
    horizon_days = row.get("horizon_days")
    predicted_direction = row.get("predicted_direction")
    if not symbol or not signal_date or not horizon_days or not predicted_direction:
        raise ValueError("Entry decision signal requires symbol, signal_date, horizon_days, and predicted_direction.")

    column_sql = ", ".join(_db_column(column) for column in _UPSERT_FIELDS)
    placeholder_sql = ", ".join(["%s"] * len(_UPSERT_FIELDS))
    update_sql = ",\n                    ".join(
        [
            "status = CASE "
            "WHEN entry_decision_signals.status = 'closed' AND EXCLUDED.status = 'open' "
            "THEN entry_decision_signals.status ELSE EXCLUDED.status END",
            *[
                f"{_db_column(column)} = EXCLUDED.{_db_column(column)}"
                for column in _UPSERT_FIELDS
                if column not in {"symbol", "signal_date", "horizon_days", "status"}
            ],
            "updated_at = NOW()",
            "closed_at = CASE "
            "WHEN entry_decision_signals.status = 'closed' AND EXCLUDED.status = 'open' "
            "THEN entry_decision_signals.closed_at "
            "WHEN EXCLUDED.status = 'closed' AND entry_decision_signals.closed_at IS NULL THEN NOW() "
            "WHEN EXCLUDED.status = 'open' THEN NULL "
            "ELSE entry_decision_signals.closed_at END",
        ]
    )

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            _ensure_table(cur)
            cur.execute(
                f"""
                    INSERT INTO entry_decision_signals ({column_sql})
                    VALUES ({placeholder_sql})
                    ON CONFLICT (symbol, signal_date, horizon_days)
                    DO UPDATE SET
                    {update_sql}
                    WHERE NOT (
                        entry_decision_signals.status = 'closed'
                        AND EXCLUDED.status = 'open'
                    );
                """,
                _row_to_params({**row, "symbol": symbol}),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def upsert_entry_decision_signals(rows: Iterable[dict]) -> int:
    count = 0
    for row in rows or []:
        upsert_entry_decision_signal(row)
        count += 1
    return count


def list_open_entry_signal_keys(symbol: str) -> set[tuple[str, int]]:
    normalized = _normalize_symbol(symbol)
    if not normalized:
        return set()

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT signal_date, horizon_days
                    FROM entry_decision_signals
                    WHERE symbol = %s AND status = 'open';
                """,
                (normalized,),
            )
            rows = cur.fetchall()
        conn.commit()
    finally:
        conn.close()

    return {(row[0].isoformat() if hasattr(row[0], "isoformat") else str(row[0])[:10], int(row[1])) for row in rows}


def close_open_entry_signals_absent_from_keys(
    symbol: str,
    *,
    open_keys: set[tuple[str, int]],
    horizon_days: Iterable[int],
    current_date=None,
) -> int:
    normalized = _normalize_symbol(symbol)
    horizons = sorted({int(value) for value in horizon_days or [] if value})
    if not normalized or not horizons:
        return 0

    params: list = [_date_text(current_date), _date_text(current_date), normalized, horizons]
    keep_sql = ""
    normalized_open_keys = {
        (_date_text(signal_date), int(horizon))
        for signal_date, horizon in open_keys or set()
        if _date_text(signal_date) and horizon
    }
    if normalized_open_keys:
        keep_clauses = []
        for signal_date, horizon in sorted(normalized_open_keys):
            keep_clauses.append("(signal_date = %s AND horizon_days = %s)")
            params.extend([signal_date, horizon])
        keep_sql = f"AND NOT ({' OR '.join(keep_clauses)})"

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                    UPDATE entry_decision_signals
                    SET
                        status = 'closed',
                        interim_status = CASE
                            WHEN interim_status IS NULL OR interim_status = '' THEN 'stale'
                            ELSE interim_status
                        END,
                        current_price_date = COALESCE(%s, current_price_date),
                        payload_as_of_date = COALESCE(%s, payload_as_of_date),
                        updated_at = NOW(),
                        closed_at = COALESCE(closed_at, NOW())
                    WHERE symbol = %s
                      AND status = 'open'
                      AND horizon_days = ANY(%s)
                      {keep_sql};
                """,
                params,
            )
            count = cur.rowcount
        conn.commit()
        return int(count or 0)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_open_entry_signal_symbols(limit: int | None = None) -> list[str]:
    params: list = []
    limit_sql = ""
    if limit is not None and limit > 0:
        limit_sql = " LIMIT %s"
        params.append(int(limit))

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                    SELECT symbol, MAX(updated_at) AS last_updated
                    FROM entry_decision_signals
                    WHERE status = 'open'
                    GROUP BY symbol
                    ORDER BY last_updated ASC, symbol
                    {limit_sql};
                """,
                params,
            )
            rows = cur.fetchall()
        conn.commit()
    finally:
        conn.close()

    return [row[0] for row in rows]


def list_open_entry_signals(symbols: Iterable[str] | None = None, limit: int = 200) -> list[dict]:
    normalized_symbols = _normalize_symbols(symbols)
    params: list = []
    symbol_filter = ""
    if symbols is not None:
        if not normalized_symbols:
            return []
        symbol_filter = " AND symbol = ANY(%s)"
        params.append(normalized_symbols)
    params.append(max(1, int(limit)))

    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                    SELECT
                        symbol,
                        signal_date,
                        horizon_days,
                        status,
                        touched_side,
                        predicted_direction,
                        trade_direction,
                        signal_close,
                        prediction_end_date,
                        current_price_date AS current_date,
                        current_close,
                        elapsed_sessions,
                        remaining_sessions,
                        progress,
                        interim_direction,
                        interim_status,
                        current_trade_return,
                        current_trade_return_atr,
                        continuation_probability,
                        reversal_probability,
                        confidence_score,
                        signal_model,
                        signal_model_id,
                        signal_precision,
                        signal_tier,
                        source,
                        model_version,
                        feature_schema_version,
                        payload_as_of_date,
                        price_data_end_date,
                        key_reasons,
                        playbook,
                        price_window,
                        updated_at
                    FROM entry_decision_signals
                    WHERE status = 'open'
                    {symbol_filter}
                    ORDER BY remaining_sessions NULLS LAST, signal_date DESC, symbol, horizon_days
                    LIMIT %s;
                """,
                params,
            )
            rows = cur.fetchall()
        conn.commit()
    finally:
        conn.close()

    return convert_to_python_types([_normalize_output_direction(dict(row)) for row in rows])
