from libby_backend.database import get_db_connection
import pandas as pd

def get_trending_books(limit=10):
    conn = get_db_connection()
    query = """
        SELECT book_id, title, author, genre, rating
        FROM books
        WHERE rating IS NOT NULL
        ORDER BY rating DESC
        LIMIT %s
    """
    df = pd.read_sql_query(query, conn, params=(limit,))
    conn.close()
    return df