# database/migrations/add_logo_column.py
import psycopg2
from db.connection import get_connection

def add_logo_url_base64_column():
    """
    Adds the 'logo_url_base64' column to the 'tickers' table if it doesn't exist.
    """
    query = """
        ALTER TABLE tickers
        ADD COLUMN IF NOT EXISTS logo_url_base64 TEXT;
    """
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()
            print("Successfully added 'logo_url_base64' column to 'tickers' table (if it didn't exist).")
    except Exception as e:
        print("Error adding column:", e)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    add_logo_url_base64_column()
