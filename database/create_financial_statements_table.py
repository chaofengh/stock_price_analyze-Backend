from database.connection import get_connection


def create_financial_statements_table():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS financial_statements (
                    id SERIAL PRIMARY KEY,
                    ticker_id INT NOT NULL,
                    statement_type VARCHAR(32) NOT NULL
                        CHECK (statement_type IN ('income_statement', 'balance_sheet', 'cash_flow')),
                    source VARCHAR(32),
                    data JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE (ticker_id, statement_type),
                    FOREIGN KEY (ticker_id) REFERENCES tickers(id) ON DELETE CASCADE
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_financial_statements_ticker
                ON financial_statements (ticker_id);
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_financial_statements_type
                ON financial_statements (statement_type);
                """
            )
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    create_financial_statements_table()
    print("financial_statements table created (if not already present).")
