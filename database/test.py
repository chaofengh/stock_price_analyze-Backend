import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def get_connection():

    
    db_url = os.getenv("DATABASE_URL")
    print(db_url)

get_connection()