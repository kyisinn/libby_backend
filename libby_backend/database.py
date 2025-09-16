# =============================================================================
# DATABASE MODULE
# =============================================================================
# Handles all PostgreSQL database connections and operations

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

# -----------------------------------------------------------------------------
# Connection
# -----------------------------------------------------------------------------
def get_db_connection():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set in env")

    try:
        conn = psycopg2.connect(
            dsn,
            cursor_factory=RealDictCursor,
            sslmode="require",
            connect_timeout=30,
            keepalives=1,
            keepalives_idle=600,
            keepalives_interval=30,
            keepalives_count=3,
            application_name="railway_book_app",
        )
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '30s'")
            cur.execute("SET idle_in_transaction_session_timeout = '60s'")
        return conn
    except Exception as e:
        print("DB connect error:", e)
        return None



# -----------------------------------------------------------------------------
# BOOK SEARCH
# -----------------------------------------------------------------------------
def search_books_db(query):
    conn = get_db_connection()
    if not conn:
        return None
    try:
        query_cleaned = query.strip().lower()
        query_words = query_cleaned.split()
        
        with conn.cursor() as cursor:
            # Build dynamic SQL with relevance scoring
            conditions = []
            params = []
            relevance_parts = []
            
            for word in query_words:
                word_pattern = f"%{word}%"
                word_exact = word
                word_start = f"{word}%"
                
                # Each word must match in at least one field
                conditions.append("(b.title ILIKE %s OR b.author ILIKE %s OR b.genre ILIKE %s)")
                params.extend([word_pattern, word_pattern, word_pattern])
                
                # Add relevance scoring (higher score = better match)
                relevance_parts.append(f"""
                    CASE WHEN LOWER(b.title) = %s THEN 100
                         WHEN LOWER(b.author) = %s THEN 90
                         WHEN LOWER(b.genre) = %s THEN 80
                         WHEN LOWER(b.title) LIKE %s THEN 70
                         WHEN LOWER(b.author) LIKE %s THEN 60
                         WHEN LOWER(b.genre) LIKE %s THEN 50
                         WHEN b.title ILIKE %s THEN 40
                         WHEN b.author ILIKE %s THEN 30
                         WHEN b.genre ILIKE %s THEN 20
                         ELSE 0
                    END
                """)
                params.extend([
                    word_exact, word_exact, word_exact,  # exact matches
                    word_start, word_start, word_start,  # starts with
                    word_pattern, word_pattern, word_pattern  # contains
                ])
            
            where_clause = " AND ".join(conditions)
            relevance_clause = " + ".join(relevance_parts)
            
            sql = f"""
                SELECT DISTINCT
                    b.book_id AS id,
                    b.title,
                    b.cover_image_url AS coverurl,
                    COALESCE(b.author, 'Unknown Author') AS author,
                    b.rating,
                    ({relevance_clause}) AS relevance_score
                FROM books b
                WHERE {where_clause}
                ORDER BY relevance_score DESC, b.rating DESC NULLS LAST
                LIMIT 50
            """
            
            cursor.execute(sql, params)
            results = cursor.fetchall()
            
            # Remove relevance_score from final results if you don't want it exposed
            return [
                {k: v for k, v in row.items() if k != 'relevance_score'}
                for row in results
            ]
            
    except Exception as e:
        print("Railway search error:", e)
        return None
    finally:
        conn.close()



# -----------------------------------------------------------------------------
# TRENDING
# -----------------------------------------------------------------------------
def get_trending_books_db(period, page, per_page):
    conn = get_db_connection()
    if not conn:
        return None

    offset = (page - 1) * per_page
    period_mapping = {
        'weekly': '7 days', '1week': '7 days',
        'monthly': '30 days', '1month': '30 days',
        '3months': '90 days', '6months': '180 days',
        'yearly': '365 days', '1year': '365 days',
        '2years': '730 days', '5years': '1825 days'
    }
    interval_period = period_mapping.get(period, '1825 days')

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                WITH trending_books AS (
                    SELECT DISTINCT
                        b.book_id AS id,
                        b.title,
                        b.cover_image_url AS coverurl,
                        b.rating,
                        CASE 
                            WHEN EXTRACT(YEAR FROM b.publication_date) > 2025
                            THEN b.publication_date - INTERVAL '543 years'
                            ELSE b.publication_date
                        END AS corrected_date,
                        COALESCE(b.author, 'Unknown Author') AS author,
                        CASE 
                            WHEN (
                                CASE 
                                    WHEN EXTRACT(YEAR FROM b.publication_date) > 2025
                                    THEN b.publication_date - INTERVAL '543 years'
                                    ELSE b.publication_date
                                END
                            ) >= CURRENT_DATE - INTERVAL %s THEN 1 
                            ELSE 2 
                        END as date_priority
                    FROM books b
                    WHERE
                        b.cover_image_url IS NOT NULL 
                        AND b.cover_image_url <> ''
                        AND b.rating IS NOT NULL
                        AND (
                            CASE 
                                WHEN EXTRACT(YEAR FROM b.publication_date) > 2025
                                THEN b.publication_date - INTERVAL '543 years'
                                ELSE b.publication_date
                            END
                        ) >= CURRENT_DATE - INTERVAL %s
                )
                SELECT id, title, coverurl, rating, author
                FROM trending_books
                ORDER BY date_priority ASC, rating DESC, corrected_date DESC NULLS LAST
                LIMIT %s OFFSET %s
            """, (interval_period, interval_period, per_page, offset))
            books = cursor.fetchall()

            # Get total count for pagination - only books matching the period filter
            cursor.execute("""
                SELECT COUNT(DISTINCT b.book_id) AS count
                FROM books b
                WHERE b.cover_image_url IS NOT NULL 
                  AND b.cover_image_url <> ''
                  AND b.rating IS NOT NULL
                  AND (
                      CASE 
                          WHEN EXTRACT(YEAR FROM b.publication_date) > 2025
                          THEN b.publication_date - INTERVAL '543 years'
                          ELSE b.publication_date
                      END
                  ) >= CURRENT_DATE - INTERVAL %s
            """, (interval_period,))
            total_books = cursor.fetchone()['count']
            
            return {'books': books, 'total_books': total_books}
    except Exception as e:
        print("Railway trending query error:", e)
        return None
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# BY MAJOR
# -----------------------------------------------------------------------------
def get_books_by_major_db(major, page, per_page):
    conn = get_db_connection()
    if not conn:
        return None

    offset = (page - 1) * per_page
    try:
        with conn.cursor() as cursor:
            search_query = f"%{major}%"
            cursor.execute("""
                SELECT DISTINCT
                    b.book_id AS id,
                    b.title,
                    b.cover_image_url AS coverurl,
                    b.rating,
                    COALESCE(b.author, 'Unknown Author') AS author
                FROM books b
                WHERE b.genre ILIKE %s
                   OR b.title ILIKE %s
                   OR b.author ILIKE %s
                ORDER BY b.rating DESC NULLS LAST
                LIMIT %s OFFSET %s
            """, (search_query, search_query, search_query, per_page, offset))
            books = cursor.fetchall()

            cursor.execute("""
                SELECT COUNT(DISTINCT b.book_id)
                FROM books b
                WHERE b.genre ILIKE %s
                   OR b.title ILIKE %s
                   OR b.author ILIKE %s
            """, (search_query, search_query, search_query))
            total_books = cursor.fetchone()['count']
            return {'books': books, 'total_books': total_books}
    except Exception as e:
        print("Railway major books error:", e)
        return None
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# SIMILARITY HELPERS
# -----------------------------------------------------------------------------
def get_similar_books_details(similar_book_ids):
    if not similar_book_ids:
        return []
    conn = get_db_connection()
    if not conn:
        return None
    try:
        with conn.cursor() as cursor:
            placeholders = ','.join(['%s'] * len(similar_book_ids))
            cursor.execute(f"""
                SELECT DISTINCT
                    b.book_id AS id,
                    b.title,
                    b.cover_image_url AS coverurl,
                    b.rating,
                    COALESCE(b.author, 'Unknown Author') AS author
                FROM books b
                WHERE b.book_id IN ({placeholders})
                ORDER BY b.rating DESC NULLS LAST
            """, tuple(similar_book_ids))
            return cursor.fetchall()
    except Exception as e:
        print("Error in get_similar_books_details:", e)
        return None
    finally:
        conn.close()


def get_books_by_genre_db(target_genre, exclude_book_id, limit=10):
    conn = get_db_connection()
    if not conn:
        return []
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT
                    b.book_id AS id,
                    b.title,
                    b.cover_image_url AS coverurl,
                    b.rating,
                    COALESCE(b.author, 'Unknown Author') AS author
                FROM books b
                WHERE b.genre ILIKE %s 
                  AND b.book_id != %s
                  AND b.cover_image_url IS NOT NULL
                ORDER BY b.rating DESC NULLS LAST
                LIMIT %s
            """, (f"%{target_genre}%", exclude_book_id, limit))
            results = cursor.fetchall()
            return [dict(row) for row in results]
    except Exception as e:
        print("Error in get_books_by_genre_db:", e)
        return []
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# BOOK BY ID
# -----------------------------------------------------------------------------
def get_book_by_id_db(book_id):
    conn = get_db_connection()
    if not conn:
        return None
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    b.*,
                    COALESCE(b.author, 'Unknown Author') AS author
                FROM books b
                WHERE b.book_id = %s
                LIMIT 1
            """, (book_id,))
            return cursor.fetchone()
    except Exception as e:
        print("Error in get_book_by_id_db:", e)
        return None
    finally:
        conn.close()

# RECORD USER INTERACTION
def record_user_interaction(user_id: int, book_id: int, interaction_type: str = "view"):
    conn = get_db_connection()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO user_interactions (user_id, book_id, interaction_type)
                VALUES (%s, %s, %s)
                RETURNING id, user_id, book_id, interaction_type, timestamp
            """, (user_id, book_id, interaction_type))
            return cur.fetchone()
    except Exception as e:
        print("record_user_interaction error:", e)
        return None
    finally:
        conn.close()
