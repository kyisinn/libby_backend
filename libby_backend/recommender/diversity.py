from libby_backend.database import get_db_connection
import pandas as pd

def get_diverse_books(preferred_genres, limit=5):
    conn = get_db_connection()
    query = """
        SELECT book_id, title, author, genre, rating
        FROM books
        WHERE genre IS NOT NULL AND genre <> ALL(%s)
        ORDER BY rating DESC
        LIMIT %s
    """
    df = pd.read_sql_query(query, conn, params=(preferred_genres, limit))
    conn.close()
    return df