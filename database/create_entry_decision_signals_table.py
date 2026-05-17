from database.connection import get_connection


ENTRY_DECISION_SIGNALS_SCHEMA_SQL = (
    """
        CREATE TABLE IF NOT EXISTS entry_decision_signals (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(16) NOT NULL,
            signal_date DATE NOT NULL,
            horizon_days INT NOT NULL,
            status VARCHAR(24) NOT NULL DEFAULT 'open',
            touched_side VARCHAR(10),
            predicted_direction VARCHAR(16) NOT NULL,
            trade_direction VARCHAR(8),
            signal_close DOUBLE PRECISION,
            current_price_date DATE,
            current_close DOUBLE PRECISION,
            outcome_date DATE,
            outcome_close DOUBLE PRECISION,
            elapsed_sessions INT,
            remaining_sessions INT,
            progress DOUBLE PRECISION,
            interim_direction VARCHAR(16),
            interim_status VARCHAR(24),
            actual_direction VARCHAR(16),
            is_correct BOOLEAN,
            current_trade_return DOUBLE PRECISION,
            current_trade_return_atr DOUBLE PRECISION,
            trade_return DOUBLE PRECISION,
            trade_return_atr DOUBLE PRECISION,
            continuation_probability DOUBLE PRECISION,
            reversal_probability DOUBLE PRECISION,
            confidence_score INT,
            signal_model TEXT,
            signal_model_id TEXT,
            signal_precision DOUBLE PRECISION,
            signal_tier TEXT,
            source VARCHAR(32) NOT NULL DEFAULT 'entry_decision',
            model_version TEXT,
            feature_schema_version TEXT,
            payload_as_of_date DATE,
            price_data_end_date DATE,
            key_reasons JSONB,
            playbook JSONB,
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
            closed_at TIMESTAMP,
            UNIQUE (symbol, signal_date, horizon_days)
        );
    """,
    """
        CREATE INDEX IF NOT EXISTS idx_entry_decision_signals_status_updated
        ON entry_decision_signals (status, updated_at DESC);
    """,
    """
        CREATE INDEX IF NOT EXISTS idx_entry_decision_signals_symbol_status
        ON entry_decision_signals (symbol, status);
    """,
    """
        CREATE INDEX IF NOT EXISTS idx_entry_decision_signals_signal_date
        ON entry_decision_signals (signal_date DESC);
    """,
    """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'entry_decision_signals'
                  AND column_name = 'current_date'
            ) AND NOT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'entry_decision_signals'
                  AND column_name = 'current_price_date'
            ) THEN
                ALTER TABLE entry_decision_signals
                RENAME COLUMN "current_date" TO current_price_date;
            END IF;
        END $$;
    """,
)


def create_entry_decision_signals_table():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for statement in ENTRY_DECISION_SIGNALS_SCHEMA_SQL:
                cur.execute(statement)
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    create_entry_decision_signals_table()
    print("entry_decision_signals table created (if not already present).")
