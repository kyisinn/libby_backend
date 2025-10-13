from libby_backend.database import get_db_connection
import pandas as pd

def get_author_recommendations(fav_authors, limit=10):
    conn = get_db_connection()
    query = f"""
        SELECT book_id, title, author, genre, rating
        FROM books
        WHERE author = ANY(%s)
        ORDER BY rating DESC
        LIMIT {limit}
    """
    df = pd.read_sql_query(query, conn, params=(fav_authors,))
    conn.close()
    return df