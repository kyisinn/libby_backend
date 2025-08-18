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
    """Create a PostgreSQL connection with sane defaults."""
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set in environment/app settings")

    try:
        # Check if we need SSL (for cloud databases like Azure)
        ssl_mode = "require" if "azure" in dsn.lower() or "amazonaws" in dsn.lower() else "prefer"
        
        conn = psycopg2.connect(
            dsn,
            cursor_factory=RealDictCursor,
            sslmode=ssl_mode,
            connect_timeout=10,
            keepalives=1,
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
        
        # Try with publication_date filter first
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
            WHERE
                b.cover_image_url IS NOT NULL AND b.cover_image_url <> ''
                AND b.publication_date >= CURRENT_DATE - {interval_sql}
            ORDER BY
                COALESCE(b.ratings_count, 0) DESC,
                b.rating DESC NULLS LAST,
                b.publication_date DESC NULLS LAST
            LIMIT %s OFFSET %s
        """
        
        cursor.execute(main_query, (per_page, offset))
        books = cursor.fetchall()
        
        # If no books found with date filter, fall back to general trending
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
                WHERE
                    b.cover_image_url IS NOT NULL AND b.cover_image_url <> ''
                    AND b.rating IS NOT NULL
                ORDER BY
                    COALESCE(b.ratings_count, 0) DESC,
                    b.rating DESC
                LIMIT %s OFFSET %s
            """
            cursor.execute(fallback_query, (per_page, offset))
            books = cursor.fetchall()
        
        # Get total count
        count_query = f"""
            SELECT COUNT(DISTINCT b.book_id) FROM books b
            WHERE b.cover_image_url IS NOT NULL AND b.cover_image_url <> ''
            AND b.publication_date >= CURRENT_DATE - {interval_sql}
        """
        
        cursor.execute(count_query)
        total_books = cursor.fetchone()['count']
        
        # If count is 0, get general count
        if total_books == 0:
            cursor.execute("""
                SELECT COUNT(*) FROM books 
                WHERE cover_image_url IS NOT NULL AND cover_image_url <> ''
                AND rating IS NOT NULL
            """)
            total_books = cursor.fetchone()['count']
        
        return {
            'books': books,
            'total_books': total_books
        }
    except Exception as e:
        print(f"Error in get_trending_books_db: {e}")
        print(f"Period requested: {period}, SQL interval: {interval_sql}")
        return None
    finally:
        cursor.close()
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
               OR COALESCE(a.first_name || ' ' || a.last_name, '') ILIKE %s
            ORDER BY b.rating DESC NULLS LAST
            LIMIT %s OFFSET %s
            """,
            (search_query, search_query, search_query, per_page, offset)
        )
        books = cursor.fetchall()
        
        cursor.execute(
            """
            SELECT COUNT(DISTINCT b.book_id) FROM books b
            LEFT JOIN book_authors ba ON b.book_id = ba.book_id
            LEFT JOIN authors a ON ba.author_id = a.author_id
            WHERE b.genre ILIKE %s
               OR b.title ILIKE %s
               OR COALESCE(a.first_name || ' ' || a.last_name, '') ILIKE %s
            """,
            (search_query, search_query, search_query)
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
        cursor.execute("""
            SELECT book_id, title, genre 
            FROM books 
            WHERE title IS NOT NULL AND genre IS NOT NULL
            ORDER BY book_id
        """)
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
        cursor.close()
        conn.close()

def get_books_by_genre_db(target_genre, exclude_book_id, limit=10):
    """Get books from the same genre for simple similarity."""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
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
              AND b.book_id != %s
              AND b.cover_image_url IS NOT NULL
            ORDER BY b.rating DESC NULLS LAST
            LIMIT %s
            """,
            (f"%{target_genre}%", exclude_book_id, limit)
        )
        results = cursor.fetchall()
        return [dict(row) for row in results]
    except Exception as e:
        print(f"Error in get_books_by_genre_db: {e}")
        return []
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
        cursor.close()
        conn.close()

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def test_db_connection():
    """Test database connection and return status."""
    conn = get_db_connection()
    if not conn:
        return {"status": "failed", "error": "Could not establish connection"}
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 as test")
        result = cursor.fetchone()
        
        # Get some basic stats
        cursor.execute("SELECT COUNT(*) as total_books FROM books")
        book_count = cursor.fetchone()['total_books']
        
        return {
            "status": "connected",
            "database": "PostgreSQL",
            "total_books": book_count
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}
    finally:
        cursor.close()
        conn.close()

def get_database_stats():
    """Get basic database statistics."""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        stats = {}
        
        # Total books
        cursor.execute("SELECT COUNT(*) as count FROM books")
        stats['total_books'] = cursor.fetchone()['count']
        
        # Books with covers
        cursor.execute("SELECT COUNT(*) as count FROM books WHERE cover_image_url IS NOT NULL AND cover_image_url <> ''")
        stats['books_with_covers'] = cursor.fetchone()['count']
        
        # Books with ratings
        cursor.execute("SELECT COUNT(*) as count FROM books WHERE rating IS NOT NULL")
        stats['books_with_ratings'] = cursor.fetchone()['count']
        
        # Average rating
        cursor.execute("SELECT AVG(rating) as avg_rating FROM books WHERE rating IS NOT NULL")
        avg_rating = cursor.fetchone()['avg_rating']
        stats['average_rating'] = round(float(avg_rating), 2) if avg_rating else 0
        
        # Total authors
        cursor.execute("SELECT COUNT(*) as count FROM authors")
        stats['total_authors'] = cursor.fetchone()['count']
        
        return stats
    except Exception as e:
        print(f"Error getting database stats: {e}")
        return None
    finally:
        cursor.close()
        conn.close()