# =============================================================================
# DATABASE MODULE - RAILWAY DEPLOYMENT
# =============================================================================
# Handles all PostgreSQL database connections and operations for Railway

import psycopg2
import os
from psycopg2.extras import RealDictCursor

# =============================================================================
# DATABASE CONNECTION
# =============================================================================

def get_db_connection():
    """Create a PostgreSQL connection optimized for Railway."""
    database_url = os.getenv("DATABASE_URL")
    
    if not database_url:
        print("ERROR: DATABASE_URL environment variable not found")
        return None

    try:
        # Railway PostgreSQL - simple connection
        conn = psycopg2.connect(
            database_url,
            cursor_factory=RealDictCursor
        )
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
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
            SELECT DISTINCT
                b.book_id AS id,
                b.title,
                b.cover_image_url AS coverurl,
                COALESCE(a.first_name || ' ' || a.last_name, 'Unknown Author') AS author,
                b.rating
            FROM books b
            LEFT JOIN book_authors ba ON b.book_id = ba.book_id
            LEFT JOIN authors a ON ba.author_id = a.author_id
            WHERE b.title ILIKE %s 
               OR COALESCE(a.first_name || ' ' || a.last_name, '') ILIKE %s
               OR b.genre ILIKE %s
            ORDER BY b.rating DESC NULLS LAST
            LIMIT 50
            """,
            (search_query, search_query, search_query)
        )
        results = cursor.fetchall()
        return results
    except Exception as e:
        print(f"Error in search_books_db: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
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
    
    # Map period parameters
    if period in ['weekly', '1week']:
        interval_sql = "INTERVAL '7 days'"
    elif period in ['monthly', '1month']:
        interval_sql = "INTERVAL '1 month'"
    elif period == '3months':
        interval_sql = "INTERVAL '3 months'"
    elif period == '6months':
        interval_sql = "INTERVAL '6 months'"
    elif period in ['yearly', '1year']:
        interval_sql = "INTERVAL '1 year'"
    elif period == '2years':
        interval_sql = "INTERVAL '2 years'"
    elif period == '5years':
        interval_sql = "INTERVAL '5 years'"
    else:
        interval_sql = "INTERVAL '5 years'"  # Default

    try:
        cursor = conn.cursor()
        
        # Try date-filtered query first
        main_query = f"""
            SELECT DISTINCT
                b.book_id AS id,
                b.title,
                b.cover_image_url AS coverurl,
                b.rating,
                b.ratings_count,
                COALESCE(a.first_name || ' ' || a.last_name, 'Unknown Author') AS author
            FROM books b
            LEFT JOIN book_authors ba ON b.book_id = ba.book_id
            LEFT JOIN authors a ON ba.author_id = a.author_id
            WHERE b.cover_image_url IS NOT NULL 
              AND b.cover_image_url <> ''
              AND b.publication_date >= CURRENT_DATE - {interval_sql}
            ORDER BY
                COALESCE(b.ratings_count, 0) DESC,
                b.rating DESC NULLS LAST
            LIMIT %s OFFSET %s
        """
        
        cursor.execute(main_query, (per_page, offset))
        books = cursor.fetchall()
        
        # Fallback to general trending if no recent books found
        if not books and offset == 0:
            fallback_query = """
                SELECT DISTINCT
                    b.book_id AS id,
                    b.title,
                    b.cover_image_url AS coverurl,
                    b.rating,
                    b.ratings_count,
                    COALESCE(a.first_name || ' ' || a.last_name, 'Unknown Author') AS author
                FROM books b
                LEFT JOIN book_authors ba ON b.book_id = ba.book_id
                LEFT JOIN authors a ON ba.author_id = a.author_id
                WHERE b.cover_image_url IS NOT NULL 
                  AND b.cover_image_url <> ''
                  AND b.rating IS NOT NULL
                ORDER BY
                    COALESCE(b.ratings_count, 0) DESC,
                    b.rating DESC
                LIMIT %s OFFSET %s
            """
            cursor.execute(fallback_query, (per_page, offset))
            books = cursor.fetchall()
        
        # Get total count
        count_query = """
            SELECT COUNT(DISTINCT b.book_id) FROM books b
            WHERE b.cover_image_url IS NOT NULL 
              AND b.cover_image_url <> ''
              AND b.rating IS NOT NULL
        """
        cursor.execute(count_query)
        total_books = cursor.fetchone()['count']
        
        return {
            'books': books,
            'total_books': total_books
        }
    except Exception as e:
        print(f"Error in get_trending_books_db: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# =============================================================================
# BOOKS BY MAJOR OPERATIONS
# =============================================================================

def get_books_by_major_db(major, page, per_page):
    """Get books by major with pagination."""
    conn = get_db_connection()
    if not conn:
        return None
    
    offset = (page - 1) * per_page
    
    try:
        cursor = conn.cursor()
        search_query = f"%{major}%"
        
        cursor.execute(
            """
            SELECT DISTINCT
                b.book_id AS id,
                b.title,
                b.cover_image_url AS coverurl,
                b.rating,
                COALESCE(a.first_name || ' ' || a.last_name, 'Unknown Author') AS author
            FROM books b
            LEFT JOIN book_authors ba ON b.book_id = ba.book_id
            LEFT JOIN authors a ON ba.author_id = a.author_id
            WHERE b.genre ILIKE %s
               OR b.title ILIKE %s
            ORDER BY b.rating DESC NULLS LAST
            LIMIT %s OFFSET %s
            """,
            (search_query, search_query, per_page, offset)
        )
        books = cursor.fetchall()
        
        # Get count
        cursor.execute(
            """
            SELECT COUNT(DISTINCT b.book_id) FROM books b
            WHERE b.genre ILIKE %s OR b.title ILIKE %s
            """,
            (search_query, search_query)
        )
        total_books = cursor.fetchone()['count']
        
        return {
            'books': books,
            'total_books': total_books
        }
    except Exception as e:
        print(f"Error in get_books_by_major_db: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# =============================================================================
# SIMILARITY OPERATIONS
# =============================================================================

def get_similar_books_details(similar_book_ids):
    """Get full details for similar books."""
    if not similar_book_ids:
        return []
        
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        placeholders = ','.join(['%s'] * len(similar_book_ids))
        query = f"""
            SELECT DISTINCT
                b.book_id AS id,
                b.title,
                b.cover_image_url AS coverurl,
                b.rating,
                COALESCE(a.first_name || ' ' || a.last_name, 'Unknown Author') AS author
            FROM books b
            LEFT JOIN book_authors ba ON b.book_id = ba.book_id
            LEFT JOIN authors a ON ba.author_id = a.author_id
            WHERE b.book_id IN ({placeholders})
            ORDER BY b.rating DESC NULLS LAST
        """
        cursor.execute(query, tuple(similar_book_ids))
        results = cursor.fetchall()
        return results
    except Exception as e:
        print(f"Error in get_similar_books_details: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
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
            LIMIT 1
            """,
            (book_id,)
        )
        book = cursor.fetchone()
        return book
    except Exception as e:
        print(f"Error in get_book_by_id_db: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()