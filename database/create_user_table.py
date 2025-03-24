# database/create_user_table.py
from database.connection import get_connection

def create_users_table():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    email VARCHAR(255) UNIQUE NOT NULL,
                    username VARCHAR(50) UNIQUE NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    reset_token VARCHAR(255),
                    reset_token_expires TIMESTAMP,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            conn.commit()
    finally:
        conn.close()

if __name__ == "__main__":
    create_users_table()
    print("users table created (if not already present).")
