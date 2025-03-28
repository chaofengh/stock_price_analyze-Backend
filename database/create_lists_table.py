# database/create_lists_table.py
from database.connection import get_connection

def create_lists_and_list_tickers_tables():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Create lists table: each row represents a list for a specific user.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lists (
                    id SERIAL PRIMARY KEY,
                    user_id INT NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    is_default BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    FOREIGN KEY (user_id) REFERENCES users(id)
                );
            """)
            # Create list_tickers table: join table for many-to-many relationship.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS list_tickers (
                    list_id INT NOT NULL,
                    ticker_id INT NOT NULL,
                    PRIMARY KEY (list_id, ticker_id),
                    FOREIGN KEY (list_id) REFERENCES lists(id),
                    FOREIGN KEY (ticker_id) REFERENCES tickers(id)
                );
            """)
        conn.commit()
    finally:
        conn.close()

if __name__ == "__main__":
    create_lists_and_list_tickers_tables()
    print("lists and list_tickers tables created (if not already present).")
