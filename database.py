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
    """Create a PostgreSQL connection optimized for Railway deployment."""
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set in environment/app settings")

    try:
        # Railway always requires SSL
        conn = psycopg2.connect(
            dsn,
            cursor_factory=RealDictCursor,
            sslmode="require",
            connect_timeout=30,  # Railway can be slower to connect
            keepalives=1,
            keepalives_idle=600,  # 10 minutes
            keepalives_interval=30,
            keepalives_count=3,
            application_name="railway_book_app"
        )
        
        # Set connection parameters optimized for Railway
        with conn.cursor() as cursor:
            cursor.execute("SET statement_timeout = '30s'")  # Prevent hanging queries
            cursor.execute("SET idle_in_transaction_session_timeout = '60s'")
            
        return conn
    except Exception as e:
        print(f"Error connecting to Railway PostgreSQL: {e}")
        return None

# =============================================================================
# BOOK SEARCH OPERATIONS
# =============================================================================

def search_books_db(query):
    """Search for books optimized for Railway."""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        with conn.cursor() as cursor:
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
        print(f"Railway search error: {e}")
        return None
    finally:
        conn.close()

# =============================================================================
# TRENDING BOOKS OPERATIONS
# =============================================================================

def get_trending_books_db(period, page, per_page):
    """Get trending books optimized for Railway deployment."""
    conn = get_db_connection()
    if not conn:
        return None
    
    offset = (page - 1) * per_page
    
    # Railway-optimized period mapping
    period_mapping = {
        'weekly': '7 days',
        '1week': '7 days',
        'monthly': '30 days',  # More predictable than '1 month'
        '1month': '30 days',
        '3months': '90 days',
        '6months': '180 days',
        'yearly': '365 days',
        '1year': '365 days',
        '2years': '730 days',
        '5years': '1825 days'
    }
    
    interval_period = period_mapping.get(period, '1825 days')  # Default 5 years

    try:
        cursor = conn.cursor()
        
        # Single optimized query for Railway - avoid multiple fallback queries
        cursor.execute("""
            WITH trending_books AS (
                SELECT DISTINCT
                    b.book_id AS id,
                    b.title,
                    b.cover_image_url AS coverurl,
                    b.rating,
                    b.publication_date,
                    COALESCE(a.first_name || ' ' || a.last_name, 'Unknown Author') AS author,
                    CASE 
                        WHEN b.publication_date >= CURRENT_DATE - INTERVAL %s THEN 1 
                        ELSE 2 
                    END as date_priority
                FROM books b
                LEFT JOIN book_authors ba ON b.book_id = ba.book_id
                LEFT JOIN authors a ON ba.author_id = a.author_id
                WHERE
                    b.cover_image_url IS NOT NULL 
                    AND b.cover_image_url <> ''
                    AND b.rating IS NOT NULL
            )
            SELECT id, title, coverurl, rating, author
            FROM trending_books
            ORDER BY 
                date_priority ASC,
                rating DESC,
                publication_date DESC NULLS LAST
            LIMIT %s OFFSET %s
        """, (interval_period, per_page, offset))
        
        books = cursor.fetchall()
        
        # Get total count efficiently
        cursor.execute("""
            SELECT COUNT(DISTINCT b.book_id) as count 
            FROM books b
            WHERE b.cover_image_url IS NOT NULL 
            AND b.cover_image_url <> ''
            AND b.rating IS NOT NULL
        """)
        
        total_result = cursor.fetchone()
        total_books = total_result['count'] if total_result else 0
        
        return {
            'books': books,
            'total_books': total_books
        }
        
    except Exception as e:
        print(f"Railway trending query error: {e}")
        return None
            
    finally:
        cursor.close()
        conn.close()

# =============================================================================
# BOOKS BY MAJOR OPERATIONS
# =============================================================================

def get_books_by_major_db(major, page, per_page):
    """Get books by major optimized for Railway."""
    conn = get_db_connection()
    if not conn:
        return None
    
    offset = (page - 1) * per_page
    
    try:
        with conn.cursor() as cursor:
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
        print(f"Railway major books error: {e}")
        return None
    finally:
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