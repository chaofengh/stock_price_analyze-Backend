# database/ticker_repository.py
from .connection import get_connection

def get_all_tickers():
    """
    Retrieve all ticker symbols from the 'tickers' table.
    Returns a list of strings, e.g. ["TSLA", "PLTR", "SQ", ...]
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT symbol FROM tickers;")
            rows = cur.fetchall()
            # rows is a list of tuples, e.g. [('TSLA',), ('PLTR',), ...]
            return [row[0] for row in rows]
    finally:
        conn.close()


def insert_tickers(ticker_list):
    """
    Inserts each symbol in ticker_list into the 'tickers' table.
    If a symbol already exists (unique constraint violation),
    the insert for that symbol is skipped.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for ticker in ticker_list:
                cur.execute("""
                    INSERT INTO tickers (symbol)
                    VALUES (%s)
                    ON CONFLICT (symbol) DO NOTHING
                """, (ticker,))
        conn.commit()
    finally:
        conn.close()

        
def remove_ticker(ticker):
    """
    Removes the given ticker symbol from the 'tickers' table.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM tickers WHERE symbol = %s", (ticker,))
        conn.commit()
    finally:
        conn.close()



