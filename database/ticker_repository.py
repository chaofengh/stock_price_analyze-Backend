# database/ticker_repository.py
from .connection import get_connection
import psycopg2


def get_all_tickers(user_id=None):
    """
    If user_id is given, return only tickers in that user's default list.
    Otherwise, return ALL tickers.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if user_id is None:
                cur.execute("SELECT symbol FROM tickers;")
            else:
                cur.execute("""
                    SELECT t.symbol
                    FROM tickers t
                    JOIN list_tickers lt ON t.id = lt.ticker_id
                    JOIN lists l ON lt.list_id = l.id
                    WHERE l.user_id = %s
                      AND l.is_default = TRUE;
                """, (user_id,))
            rows = cur.fetchall()
            return [row[0] for row in rows]
    finally:
        conn.close()



def create_default_user_list(user_id):
    """
    Creates a default list for a new user and populates it with the predefined tickers.
    Returns the ID of the new list.
    """
    # Define the default tickers you want every new user to have.
    default_tickers = [
        "TSLA", "PLTR", "NFLX", "AMD", "MU", "CRWD", "SHOP", "META", "GS",
        "NVDA", "PYPL", "SPOT", "ABNB", "CRM", "UBER", "ZM", "TGT", "ADBE", "AMZN",
        "HD", "PINS", "AAPL", "BBY", "V", "COST", "WMT", "MSFT", "DIS", "SBUX",
        "JPM", "LULU", "MCD", "T", "QQQ", "XYZ", "TQQQ", "NVDL", "SSO"
    ]
    
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Create the default list for this user.
            cur.execute("""
                INSERT INTO lists (user_id, name, is_default)
                VALUES (%s, %s, TRUE)
                RETURNING id;
            """, (user_id, "Default List"))
            list_id = cur.fetchone()[0]
            
            # Ensure each default ticker exists in the tickers table.
            for ticker in default_tickers:
                cur.execute("""
                    INSERT INTO tickers (symbol)
                    VALUES (%s)
                    ON CONFLICT (symbol) DO NOTHING;
                """, (ticker,))
            
            # Get the ticker IDs for the default tickers.
            cur.execute("""
                SELECT id, symbol FROM tickers
                WHERE symbol = ANY(%s);
            """, (default_tickers,))
            ticker_rows = cur.fetchall()
            
            # Insert each ticker into the list_tickers table for the new list.
            for ticker_id, symbol in ticker_rows:
                cur.execute("""
                    INSERT INTO list_tickers (list_id, ticker_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING;
                """, (list_id, ticker_id))
        conn.commit()
        return list_id
    finally:
        conn.close()

# database/ticker_repository.py

def add_ticker_to_user_list(user_id, symbol):
    """
    1) Ensure 'symbol' exists in the global 'tickers' table (insert if not).
    2) Insert a row into list_tickers linking the user's default list to that ticker.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Ensure ticker exists globally
            cur.execute("""
                INSERT INTO tickers (symbol)
                VALUES (%s)
                ON CONFLICT (symbol) DO NOTHING
            """, (symbol,))

            # Get the ticker_id
            cur.execute("SELECT id FROM tickers WHERE symbol = %s", (symbol,))
            row = cur.fetchone()
            if not row:
                raise Exception(f"Failed to insert/find ticker {symbol}")
            ticker_id = row[0]

            # Get or create the user's default list (if you always have exactly one)
            cur.execute("""
                SELECT id FROM lists
                WHERE user_id = %s AND is_default = TRUE
                LIMIT 1
            """, (user_id,))
            default_list = cur.fetchone()
            if not default_list:
                # If the user doesn't have a default list, create one
                cur.execute("""
                    INSERT INTO lists (user_id, name, is_default)
                    VALUES (%s, %s, TRUE)
                    RETURNING id
                """, (user_id, "Default List"))
                default_list_id = cur.fetchone()[0]
            else:
                default_list_id = default_list[0]

            # Link the ticker to the user's default list
            cur.execute("""
                INSERT INTO list_tickers (list_id, ticker_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (default_list_id, ticker_id))

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def remove_ticker_from_user_list(user_id, symbol):
    """
    Remove 'symbol' from the user's default list_tickers only.
    Does NOT remove it from the global tickers table.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Find the user's default list
            cur.execute("""
                SELECT id FROM lists
                WHERE user_id = %s AND is_default = TRUE
                LIMIT 1
            """, (user_id,))
            default_list = cur.fetchone()
            if not default_list:
                # The user doesn't have a default list or doesn't exist
                return
            default_list_id = default_list[0]

            # Find the ticker ID
            cur.execute("SELECT id FROM tickers WHERE symbol = %s", (symbol,))
            row = cur.fetchone()
            if not row:
                # Ticker not found in global table
                return
            ticker_id = row[0]

            # Delete from list_tickers for that user
            cur.execute("""
                DELETE FROM list_tickers
                WHERE list_id = %s AND ticker_id = %s
            """, (default_list_id, ticker_id))

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()
def get_logo_base64_for_symbol(symbol):
    """
    Returns the Base64-encoded logo for the given symbol, or None if not set.
    """
    query = """
        SELECT logo_url_base64
        FROM tickers
        WHERE symbol = %s
        LIMIT 1;
    """
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(query, (symbol,))
            result = cur.fetchone()
            return result[0] if result else None
    except Exception as e:
        print(f"Error fetching logo_url_base64 for symbol {symbol}:", e)
        return None
    finally:
        if conn:
            conn.close()

def update_logo_base64_for_symbol(symbol, logo_base64):
    """
    Updates the 'logo_url_base64' column for the given symbol.
    If the ticker doesn't exist, you could either create it or handle the error.
    """
    query = """
        UPDATE tickers
        SET logo_url_base64 = %s
        WHERE symbol = %s
    """
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(query, (logo_base64, symbol))
            if cur.rowcount == 0:
                # If no row was updated, symbol might not exist yet
                # Insert a new record if desired
                insert_query = """
                    INSERT INTO tickers (symbol, logo_url_base64)
                    VALUES (%s, %s)
                """
                cur.execute(insert_query, (symbol, logo_base64))
            conn.commit()
    except Exception as e:
        print(f"Error updating logo_url_base64 for symbol {symbol}:", e)
    finally:
        if conn:
            conn.close()
