# =============================================================================
# DATABASE MODULE
# =============================================================================
# Handles all PostgreSQL database connections and operations

import psycopg2
import os
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# DATABASE CONNECTION
# =============================================================================

def get_db_connection():
    """Create a PostgreSQL connection with sane defaults for Azure."""
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set in environment/app settings")

    try:
        conn = psycopg2.connect(
            dsn,
            cursor_factory=RealDictCursor,
            sslmode="require",           # enforce TLS even if URL lacks it
            connect_timeout=5,           # fail quickly if blocked by firewall
            keepalives=1,                # keep connection alive on Azure
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

# =============================================================================
# BOOK SEARCH OPERATIONS
# =============================================================================

def search_books_db(query):
    """Search for books in the database."""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        search_query = f"%{query}%"
        cursor.execute(
            """
            SELECT
                b.book_id AS id,
                b.title,
                b.cover_image_url AS coverurl,
                COALESCE(a.first_name || ' ' || a.last_name, 'Unknown Author') AS author
            FROM books b
            LEFT JOIN book_authors ba ON b.book_id = ba.book_id
            LEFT JOIN authors a ON ba.author_id = a.author_id
            WHERE b.title ILIKE %s OR (a.first_name || ' ' || a.last_name) ILIKE %s
            """,
            (search_query, search_query)
        )
        results = cursor.fetchall()
        return results
    except Exception as e:
        print(f"Error in search_books_db: {e}")
        return None
    finally:
        cursor.close()
        conn.close()

# =============================================================================
# TRENDING BOOKS OPERATIONS
# =============================================================================

def get_trending_books_db(period, page, per_page):
    """Get trending books based on time period."""
    conn = get_db_connection()
    if not conn:
        return None
    
    offset = (page - 1) * per_page
    
    # Fix the period parameter mapping
    if period == 'weekly' or period == '1week':
        interval_sql = "INTERVAL '7 days'"
    elif period == 'monthly' or period == '1month':
        interval_sql = "INTERVAL '1 month'"
    elif period == '3months':
        interval_sql = "INTERVAL '3 months'"
    elif period == '6months':
        interval_sql = "INTERVAL '6 months'"
    elif period == 'yearly' or period == '1year':
        interval_sql = "INTERVAL '1 year'"
    elif period == '2years':
        interval_sql = "INTERVAL '2 years'"
    elif period == '5years':
        interval_sql = "INTERVAL '5 years'"
    else:
        # Default to 5 years for unknown periods
        interval_sql = "INTERVAL '5 years'"

    try:
        cursor = conn.cursor()
        
        main_query = f"""
            SELECT
                b.book_id AS id,
                b.title,
                b.cover_image_url AS coverurl,
                b.rating,
                COALESCE(a.first_name || ' ' || a.last_name, 'Unknown Author') AS author
            FROM books b
            LEFT JOIN book_authors ba ON b.book_id = ba.book_id
            LEFT JOIN authors a ON ba.author_id = a.author_id
            WHERE
                b.cover_image_url IS NOT NULL AND b.cover_image_url <> ''
                AND b.publication_date >= CURRENT_DATE - {interval_sql}
            ORDER BY
                b.rating DESC NULLS LAST,
                b.publication_date DESC
            LIMIT %s OFFSET %s
        """
        
        count_query = f"""
            SELECT COUNT(*) FROM books 
            WHERE cover_image_url IS NOT NULL AND cover_image_url <> ''
            AND publication_date >= CURRENT_DATE - {interval_sql}
        """

        cursor.execute(main_query, (per_page, offset))
        books = cursor.fetchall()
        
        cursor.execute(count_query)
        total_books = cursor.fetchone()['count']
        
        return {
            'books': books,
            'total_books': total_books
        }
    except Exception as e:
        print(f"Error in get_trending_books_db: {e}")
        print(f"Period requested: {period}, SQL interval: {interval_sql}")  # Debug info
        return None
    finally:
        cursor.close()
        conn.close()

# =============================================================================
# SIMILARITY OPERATIONS
# =============================================================================

def get_all_books_for_similarity():
    """Get all books for similarity calculations."""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT book_id, title, genre FROM books WHERE title IS NOT NULL AND genre IS NOT NULL")
        all_books = cursor.fetchall()
        return all_books
    except Exception as e:
        print(f"Error in get_all_books_for_similarity: {e}")
        return None
    finally:
        cursor.close()
        conn.close()

def get_similar_books_details(similar_book_ids):
    """Get full details for similar books."""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        placeholders = ','.join(['%s'] * len(similar_book_ids))
        query = f"""
            SELECT
                b.book_id AS id,
                b.title,
                b.cover_image_url AS coverurl,
                COALESCE(a.first_name || ' ' || a.last_name, 'Unknown Author') AS author
            FROM books b
            LEFT JOIN book_authors ba ON b.book_id = ba.book_id
            LEFT JOIN authors a ON ba.author_id = a.author_id
            WHERE b.book_id IN ({placeholders})
        """
        cursor.execute(query, tuple(similar_book_ids))
        results = cursor.fetchall()
        return results
    except Exception as e:
        print(f"Error in get_similar_books_details: {e}")
        return None
    finally:
        cursor.close()
        conn.close()

# =============================================================================
# INDIVIDUAL BOOK OPERATIONS
# =============================================================================

def get_book_by_id_db(book_id):
    """Get a single book by its ID."""
    conn = get_db_connection()
    if not conn:
        return None

    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                b.*,
                COALESCE(a.first_name || ' ' || a.last_name, 'Unknown Author') AS author
            FROM books b
            LEFT JOIN book_authors ba ON b.book_id = ba.book_id
            LEFT JOIN authors a ON ba.author_id = a.author_id
            WHERE b.book_id = %s
            """,
            (book_id,)
        )
        book = cursor.fetchone()
        return book
    except Exception as e:
        print(f"Error in get_book_by_id_db: {e}")
        return None
    finally:
        cursor.close()
        conn.close()