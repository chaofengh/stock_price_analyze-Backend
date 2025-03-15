# db/connection.py
import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def get_connection():
    """
    Establish and return a psycopg2 connection using an environment variable.
    You can also hard-code or read from .env here if you prefer.
    """
    # Example: read from .env or environment
    # (Render sets DATABASE_URL, or you can set your own var, e.g. DB_URL)
    
    db_url = os.getenv("DATABASE_URL")
    
    if not db_url:
        # Fallback to your Render-provided URL if you prefer
        db_url = "postgresql://stock_analyze_db_user:YOUR_PASSWORD@..."
    
    return psycopg2.connect(db_url, sslmode="require")  # Render often requires sslmode

