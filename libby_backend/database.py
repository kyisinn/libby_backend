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
# USER AUTH OPERATIONS  (matches your existing users table)
# Columns: user_id, first_name, last_name, email, phone, membership_type, is_active
# Added: password_hash, created_at (make sure these columns exist)
# -----------------------------------------------------------------------------
def get_user_by_email(email: str):
    conn = get_db_connection()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT user_id, email, password_hash, first_name, last_name, phone,
                       membership_type, is_active, created_at
                FROM users
                WHERE lower(email) = lower(%s)
                LIMIT 1
            """, (email,))
            return cur.fetchone()
    except Exception as e:
        print("get_user_by_email error:", e)
        return None
    finally:
        conn.close()

def get_user_by_id(user_id: int):
    conn = get_db_connection()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT user_id, email, first_name, last_name, phone,
                       membership_type, is_active, created_at
                FROM users
                WHERE user_id = %s
                LIMIT 1
            """, (user_id,))
            return cur.fetchone()
    except Exception as e:
        print("get_user_by_id error:", e)
        return None
    finally:
        conn.close()

def create_user(email: str, password_hash: str,
                first_name: str | None, last_name: str | None,
                phone: str | None):
    conn = get_db_connection()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (email, password_hash, first_name, last_name, phone, is_active)
                VALUES (%s, %s, %s, %s, %s, TRUE)
                RETURNING user_id, email, first_name, last_name, phone,
                          membership_type, is_active, created_at
            """, (email, password_hash, first_name, last_name, phone))
            user = cur.fetchone()
            conn.commit()
            return user
    except Exception as e:
        print("create_user error:", e)
        # Handle duplicate email robustly
        if "duplicate key value" in str(e).lower() or "unique constraint" in str(e).lower():
            return {"_duplicate": True}
        return None
    finally:
        conn.close()

def update_user_full_name(user_id: int, full_name: str) -> bool:
  conn = get_db_connection()
  if not conn:
    return False
  try:
    with conn.cursor() as cur:
      cur.execute("UPDATE users SET full_name = %s WHERE user_id = %s", (full_name, user_id))
      conn.commit()
      return True
  except Exception as e:
    print("update_user_full_name error:", e)
    return False
  finally:
    conn.close()

# -----------------------------------------------------------------------------
# BOOK SEARCH
# -----------------------------------------------------------------------------
def search_books_db(query):
    conn = get_db_connection()
    if not conn:
        return None
    try:
        with conn.cursor() as cursor:
            search_query = f"%{query}%"
            cursor.execute("""
                SELECT DISTINCT
                    b.book_id AS id,
                    b.title,
                    b.cover_image_url AS coverurl,
                    COALESCE(b.author, 'Unknown Author') AS author,
                    b.rating
                FROM books b
                WHERE b.title ILIKE %s 
                   OR b.author ILIKE %s
                   OR b.genre ILIKE %s
                ORDER BY b.rating DESC NULLS LAST
                LIMIT 50
            """, (search_query, search_query, search_query))
            return cursor.fetchall()
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
                        b.publication_date,
                        COALESCE(b.author, 'Unknown Author') AS author,
                        CASE 
                            WHEN b.publication_date >= CURRENT_DATE - INTERVAL %s THEN 1 
                            ELSE 2 
                        END as date_priority
                    FROM books b
                    WHERE
                        b.cover_image_url IS NOT NULL 
                        AND b.cover_image_url <> ''
                        AND b.rating IS NOT NULL
                )
                SELECT id, title, coverurl, rating, author
                FROM trending_books
                ORDER BY date_priority ASC, rating DESC, publication_date DESC NULLS LAST
                LIMIT %s OFFSET %s
            """, (interval_period, per_page, offset))
            books = cursor.fetchall()

            cursor.execute("""
                SELECT COUNT(DISTINCT b.book_id) AS count
                FROM books b
                WHERE b.cover_image_url IS NOT NULL 
                  AND b.cover_image_url <> ''
                  AND b.rating IS NOT NULL
            """)
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
