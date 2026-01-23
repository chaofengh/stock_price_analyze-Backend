# database/migrations/add_price_movement_columns.py
from database.connection import get_connection


def add_price_movement_columns():
    query = """
        ALTER TABLE tickers
        ADD COLUMN IF NOT EXISTS price_movement_data JSONB,
        ADD COLUMN IF NOT EXISTS price_movement_updated_at TIMESTAMP;
    """
    index_query = """
        CREATE INDEX IF NOT EXISTS idx_tickers_price_movement_updated_at
        ON tickers (price_movement_updated_at);
    """
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(query)
            cur.execute(index_query)
            conn.commit()
            print("Successfully added price_movement columns to tickers table (if missing).")
    except Exception as exc:
        print("Error adding price_movement columns:", exc)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    add_price_movement_columns()
