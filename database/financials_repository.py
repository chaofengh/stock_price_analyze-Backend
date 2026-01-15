import json
from psycopg2.extras import Json

from .connection import get_connection

STATEMENT_TYPES = {"income_statement", "balance_sheet", "cash_flow"}


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper() if symbol else ""


def get_financial_statement(symbol: str, statement_type: str):
    symbol = _normalize_symbol(symbol)
    if not symbol:
        return None
    if statement_type not in STATEMENT_TYPES:
        raise ValueError(f"Invalid statement type: {statement_type}")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT fs.data
                FROM financial_statements fs
                JOIN tickers t ON fs.ticker_id = t.id
                WHERE t.symbol = %s
                  AND fs.statement_type = %s
                LIMIT 1;
                """,
                (symbol, statement_type),
            )
            row = cur.fetchone()
            if not row:
                return None
            payload = row[0]
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except Exception:
                    payload = None
            return payload
    finally:
        conn.close()


def upsert_financial_statement(
    symbol: str,
    statement_type: str,
    payload: dict,
    source=None,
):
    symbol = _normalize_symbol(symbol)
    if not symbol or payload is None:
        return
    if statement_type not in STATEMENT_TYPES:
        raise ValueError(f"Invalid statement type: {statement_type}")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tickers (symbol)
                VALUES (%s)
                ON CONFLICT (symbol) DO NOTHING;
                """,
                (symbol,),
            )
            cur.execute(
                "SELECT id FROM tickers WHERE symbol = %s LIMIT 1;",
                (symbol,),
            )
            row = cur.fetchone()
            if not row:
                raise Exception(f"Ticker not found for symbol {symbol}")
            ticker_id = row[0]

            cur.execute(
                """
                INSERT INTO financial_statements (ticker_id, statement_type, source, data)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (ticker_id, statement_type)
                DO UPDATE SET data = EXCLUDED.data,
                              source = EXCLUDED.source,
                              updated_at = NOW();
                """,
                (ticker_id, statement_type, source, Json(payload)),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
