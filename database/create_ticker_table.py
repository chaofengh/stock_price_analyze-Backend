#creatre_ticker_table.py
from database.connection import get_connection

def create_tickers_table():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tickers (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL UNIQUE,
                    logo_url_base64 TEXT
                );
            """)
            conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    create_tickers_table()
    print("tickers table created (if not already present).")
