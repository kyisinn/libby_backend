# =============================================================================
# DATABASE MODULE
# =============================================================================
# Handles all PostgreSQL database connections and operations

import os
import psycopg2
import psycopg2.errors
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from sqlalchemy import create_engine
import traceback
from typing import Optional, List
from libby_backend.utils.user_resolver import resolve_user_id
import logging

load_dotenv()

logger = logging.getLogger(__name__)

# SQLAlchemy engine for recommendation system
engine = create_engine(
    os.getenv("DATABASE_URL"),
    pool_pre_ping=True,
    pool_recycle=300,
    echo=False  # Set to True for SQL debugging
)

# -----------------------------------------------------------------------------
# Database Table Creation
# -----------------------------------------------------------------------------
def create_user_interactions_table():
    """Create the user_interactions table if it doesn't exist"""
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            # First check if table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'user_interactions'
                );
            """)
            result = cur.fetchone()
            table_exists = result['exists'] if result else False
            
            if table_exists:
                print("user_interactions table already exists")
                return True
            
            # Create the table with a simpler approach
            cur.execute("""
                CREATE TABLE user_interactions (
                    id SERIAL PRIMARY KEY,
                    clerk_user_id TEXT,
                    user_id INTEGER NOT NULL,
                    book_id INTEGER NOT NULL,
                    interaction_type VARCHAR(50) NOT NULL DEFAULT 'view',
                    rating DECIMAL(3,2) DEFAULT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Create indexes separately
            cur.execute("CREATE INDEX idx_user_interactions_user_id ON user_interactions(user_id);")
            cur.execute("CREATE INDEX idx_user_interactions_book_id ON user_interactions(book_id);")
            cur.execute("CREATE INDEX idx_user_interactions_timestamp ON user_interactions(timestamp);")
            
            conn.commit()
            print("user_interactions table created successfully")
            return True
    except Exception as e:
        print(f"Error creating user_interactions table: {e}")
        print(f"Error type: {type(e)}")
        conn.rollback()
        return False
    finally:
        conn.close()

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
        query_words = query.strip().split()
        phrase = query.strip()
        phrase_pattern = f"%{phrase}%"
        is_multi_word = len(query_words) >= 2
        
        with conn.cursor() as cursor:
            # Build conditions for exact word matches vs partial matches
            # Build phrase-level condition (rank 0):
            # - multi-word → ILIKE "%machine learning%"
            # - single-word → regex word boundary \mjava\M (prevents "javascript")
            if is_multi_word:
                phrase_pattern = f"%{phrase}%"
                phrase_condition = "(b.title ILIKE %s OR b.author ILIKE %s OR b.genre ILIKE %s)"
                phrase_params = [phrase_pattern, phrase_pattern, phrase_pattern]
            else:
                boundary = f"\\m{phrase}\\M"  # word-boundary regex
                phrase_condition = "(b.title ~* %s OR b.author ~* %s OR b.genre ~* %s)"
                phrase_params = [boundary, boundary, boundary]
        

            exact_conditions = []
            partial_conditions = []
            params = []
            
            for word in query_words:
                # Exact word regex (higher priority)
                exact_regex = f'\\m{word}\\M'
                exact_conditions.append("(b.title ~* %s OR b.author ~* %s OR b.genre ~* %s)")
                params.extend([exact_regex, exact_regex, exact_regex])

                # Fuzzy match (lower priority)
                word_pattern = f"%{word}%"
                
                # Partial match conditions (fallback)
                partial_conditions.append("(b.title ILIKE %s OR b.author ILIKE %s OR b.genre ILIKE %s)")
                params.extend([word_pattern, word_pattern, word_pattern])
            
            # Combine conditions: prefer exact matches, allow partial as fallback
            exact_clause = " AND ".join(exact_conditions)
            partial_clause = " AND ".join(partial_conditions)
            
            sql = f"""
                WITH phrase_matches AS (
                    SELECT DISTINCT
                        b.book_id AS id,
                        b.title,
                        b.cover_image_url AS coverurl,
                        COALESCE(b.author, 'Unknown Author') AS author,
                        b.rating,
                        0 as match_type
                    FROM books b
                    WHERE {phrase_condition}
                ),
                exact_matches AS (
                    SELECT DISTINCT
                        b.book_id AS id,
                        b.title,
                        b.cover_image_url AS coverurl,
                        COALESCE(b.author, 'Unknown Author') AS author,
                        b.rating,
                        1 as match_type
                    FROM books b
                    WHERE {exact_clause}
                    AND b.book_id NOT IN (SELECT id FROM phrase_matches)
                ),
                partial_matches AS (
                    SELECT DISTINCT
                        b.book_id AS id,
                        b.title,
                        b.cover_image_url AS coverurl,
                        COALESCE(b.author, 'Unknown Author') AS author,
                        b.rating,
                        2 as match_type
                    FROM books b
                    WHERE {partial_clause}
                    AND b.book_id NOT IN (SELECT id FROM phrase_matches)
                    AND b.book_id NOT IN (SELECT id FROM exact_matches)
                )
                SELECT *
                FROM (
                    SELECT * FROM phrase_matches
                    UNION ALL
                    SELECT * FROM exact_matches
                    UNION ALL
                    SELECT * FROM partial_matches
                ) combined
                ORDER BY match_type ASC, rating DESC NULLS LAST
                LIMIT 50;
            """
            
            cursor.execute(sql, params+phrase_params)
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
    # Updated period mapping: weekly → current year, monthly → last 2 years, yearly → last 5 years
    period_mapping = {
        'weekly': 0, '1week': 0,
        'monthly': 2, '1month': 2,
        'yearly': 5, '1year': 5,
        '2years': 7, '5years': 10
    }
    years = period_mapping.get(period, 5)

    try:
        with conn.cursor() as cursor:
            # If years == 0, filter only current year
            if years == 0:
                cursor.execute("""
                    SELECT DISTINCT
                        b.book_id AS id,
                        b.title,
                        b.cover_image_url AS cover_image_url,
                        b.rating,
                        COALESCE(b.author, 'Unknown Author') AS author,
                        b.publication_date AS year
                    FROM books b
                    WHERE b.publication_date = EXTRACT(YEAR FROM CURRENT_DATE)
                    AND b.cover_image_url IS NOT NULL
                    AND b.rating IS NOT NULL
                    ORDER BY b.rating DESC NULLS LAST, b.publication_date DESC NULLS LAST
                    LIMIT %s OFFSET %s;
                """, (per_page, offset))
            else:
                cursor.execute("""
                    WITH trending_books AS (
                        SELECT DISTINCT
                            b.book_id AS id,
                            b.title,
                            b.cover_image_url AS cover_image_url,
                            b.rating,
                            CASE 
                                WHEN b.publication_date > 2025 THEN b.publication_date - 543
                                ELSE b.publication_date
                            END AS corrected_date,
                            COALESCE(b.author, 'Unknown Author') AS author,
                            CASE 
                                WHEN (
                                    CASE 
                                        WHEN b.publication_date > 2025 THEN b.publication_date - 543
                                        ELSE b.publication_date
                                    END
                                ) >= (EXTRACT(YEAR FROM CURRENT_DATE) - %s) THEN 1 
                                ELSE 2 
                            END as date_priority
                        FROM books b
                        WHERE
                            b.cover_image_url IS NOT NULL 
                            AND b.cover_image_url <> ''
                            AND (
                                CASE 
                                    WHEN b.publication_date > 2025 THEN b.publication_date - 543
                                    ELSE b.publication_date
                                END
                            ) >= (EXTRACT(YEAR FROM CURRENT_DATE) - %s)
                    )
                    SELECT id, title, cover_image_url, rating, author, corrected_date AS publication_date
                    FROM trending_books
                    ORDER BY date_priority ASC, rating DESC, corrected_date DESC NULLS LAST
                    LIMIT %s OFFSET %s;
                """, (years, years, per_page, offset))
            books = cursor.fetchall()

            # Total count
            cursor.execute("SELECT COUNT(*) AS count FROM books;")
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
                    b.cover_image_url AS cover_image_url,
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
                    b.cover_image_url AS cover_image_url,
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
                    b.cover_image_url AS cover_image_url,
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



def _resolve_numeric_user_id(conn, user_id: Optional[int], clerk_user_id: Optional[str]) -> Optional[int]:
    """Resolve a deterministic numeric user_id from either an integer or a clerk_user_id.

    Prefer the provided numeric user_id. If missing, try `resolve_user_id` helper.
    If that fails, attempt to look up an existing mapping in user_interactions.
    """
    if user_id:
        return user_id
    if clerk_user_id:
        try:
            # Prefer centralized resolver if available
            return resolve_user_id(clerk_user_id)
        except Exception:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT user_id FROM public.user_interactions WHERE clerk_user_id = %s LIMIT 1", (clerk_user_id,))
                    row = cur.fetchone()
                    if row and row.get('user_id'):
                        return row['user_id']
            except Exception:
                pass
    return None


def record_user_interaction_db(
    user_id: Optional[int],
    book_id: int,
    interaction_type: str = "view",
    rating: float = None,
    clerk_user_id: Optional[str] = None
):
    """
    Record interaction into Postgres user_interactions.
    Accepts either numeric user_id or clerk_user_id (or both).
    """
    conn = get_db_connection()
    if not conn:
        return None
    try:
        numeric_user_id = _resolve_numeric_user_id(conn, user_id, clerk_user_id)

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO public.user_interactions 
                    (user_id, clerk_user_id, book_id, interaction_type, rating, timestamp)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                RETURNING id, user_id, clerk_user_id, book_id, interaction_type, rating, timestamp
            """, (numeric_user_id, clerk_user_id, book_id, interaction_type, rating))
            row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None
    except Exception as e:
        print("record_user_interaction_db error:", e)
        conn.rollback()
        return None
    finally:
        if conn and not conn.closed:
            conn.close()

def get_user_interactions_db(user_id: Optional[int] = None,
                             clerk_user_id: Optional[str] = None,
                             limit: int = 100) -> List[dict]:
    """Get user interaction history with book details (Postgres)."""
    conn = get_db_connection()
    if not conn:
        return []

    try:
        where, params = [], []
        if user_id is not None:
            where.append("ui.user_id = %s")
            params.append(int(user_id))
        if clerk_user_id:
            where.append("ui.clerk_user_id = %s")
            params.append(str(clerk_user_id))

        if not where:
            return []  # avoid full scan

        sql = f"""
            SELECT ui.id, ui.user_id, ui.clerk_user_id, ui.book_id,
                   ui.interaction_type, ui.rating, ui.timestamp,
                   b.book_id, b.title, b.author, b.genre, b.cover_image_url
            FROM public.user_interactions ui
            LEFT JOIN public.books b ON b.book_id = ui.book_id
            WHERE {' OR '.join(where)}
            ORDER BY ui.timestamp DESC
            LIMIT %s
        """
        params.append(int(limit))

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    except Exception as e:
        print("get_user_interactions_db error:", e)
        return []
    finally:
        conn.close()

# --- Collaborative filtering on Postgres ------------------

def collaborative_filtering_recommendations_pg(clerk_user_id: Optional[str], user_id: Optional[int], limit: int = 10):
    """
    Recommend books liked/viewed by similar users (overlap >= 2),
    weighting by interaction type and number of common books.
    """
    conn = get_db_connection()
    if not conn:
        return [], []

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Use either numeric id or clerk id to identify "me"
            # Positive interactions considered:
            positives = ("view","like","wishlist_add","rate")

            # CTEs:
            # 1) my_books: books I've positively interacted with
            # 2) sim: users who share >=2 of those books with me
            # 3) rec: candidate books from similar users that I haven't interacted with,
            #         scored by interaction weight * similarity factor
            cur.execute(f"""
                WITH me AS (
                    SELECT %s::BIGINT AS uid, %s::TEXT AS cid
                ),
                my_books AS (
                    SELECT DISTINCT ui.book_id
                    FROM public.user_interactions ui, me
                    WHERE (ui.user_id = me.uid OR ui.clerk_user_id = me.cid)
                      AND ui.interaction_type = ANY(%s)
                ),
                others AS (
                    SELECT
                        COALESCE(ui2.user_id::TEXT, ui2.clerk_user_id) AS other_key,
                        COUNT(*) AS common_books
                    FROM public.user_interactions ui1
                    JOIN public.user_interactions ui2
                      ON ui1.book_id = ui2.book_id
                    JOIN me ON TRUE
                    WHERE (ui1.user_id = me.uid OR ui1.clerk_user_id = me.cid)
                      AND ui1.interaction_type = ANY(%s)
                      AND ui2.interaction_type = ANY(%s)
                      AND COALESCE(ui1.user_id::TEXT, ui1.clerk_user_id)
                          <> COALESCE(ui2.user_id::TEXT, ui2.clerk_user_id)
                    GROUP BY 1
                    HAVING COUNT(*) >= 2
                ),
                rec AS (
                    SELECT ui.book_id,
                           SUM(
                               CASE ui.interaction_type
                                   WHEN 'view'         THEN 1.0
                                   WHEN 'like'         THEN 2.0
                                   WHEN 'wishlist_add' THEN 3.0
                                   WHEN 'rate'         THEN 1.5 + COALESCE(ui.rating,0)/10.0
                                   ELSE 0.5
                               END * (1 + LEAST(others.common_books/5.0, 1.0))
                           ) AS score
                    FROM public.user_interactions ui
                    JOIN others
                      ON COALESCE(ui.user_id::TEXT, ui.clerk_user_id) = others.other_key
                    WHERE ui.book_id NOT IN (SELECT book_id FROM my_books)
                    GROUP BY ui.book_id
                )
                SELECT b.book_id, b.title, b.author, b.genre, b.cover_image_url, b.rating, rec.score
                FROM rec
                JOIN public.books b ON b.book_id = rec.book_id
                ORDER BY rec.score DESC
                LIMIT %s
            """, (
                user_id, clerk_user_id, list(positives),
                list(positives), list(positives), limit
            ))
            rows = cur.fetchall()

        reasons = []
        if rows:
            reasons.append("Users with similar reading preferences also engaged with these books")
        return rows, reasons

    except Exception as e:
        print("Error in collaborative_filtering_recommendations_pg:", e)
        return [], []
    finally:
        conn.close()

def get_collaborative_recommendations_db(user_id: Optional[int] = None, limit: int = 20, clerk_user_id: Optional[str] = None):
    """Compatibility wrapper: return just rows like previous API."""
    rows, _ = collaborative_filtering_recommendations_pg(clerk_user_id=clerk_user_id, user_id=user_id, limit=limit)
    return rows

def get_user_genre_preferences_db(user_id: int):
    """Get user's preferred genres based on interaction history"""
    conn = get_db_connection()
    if not conn:
        return []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT b.genre, COUNT(*) as interaction_count,
                       AVG(CASE 
                           WHEN ui.interaction_type = 'click' THEN 1
                           WHEN ui.interaction_type = 'view' THEN 2  
                           WHEN ui.interaction_type = 'wishlist_add' THEN 3
                           ELSE 1 END) as preference_score
                FROM user_interactions ui
                JOIN books b ON ui.book_id = b.book_id
                WHERE ui.user_id = %s 
                AND b.genre IS NOT NULL
                AND ui.interaction_type IN ('click', 'view', 'wishlist_add')
                GROUP BY b.genre
                HAVING COUNT(*) >= 2
                ORDER BY preference_score DESC, interaction_count DESC
                LIMIT 10
            """, (user_id,))
            return cur.fetchall()
    except psycopg2.errors.UndefinedTable:
        print("user_interactions table doesn't exist for genre preferences, creating it...")
        create_user_interactions_table()
        return []  # Return empty list for now
    except Exception as e:
        print("get_user_genre_preferences_db error:", e)
        return []
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Tiny helpers
# -----------------------------------------------------------------------------
def count_user_interactions(clerk_user_id: str) -> int:
    """Return number of rows in user_interactions for a clerk_user_id."""
    if not clerk_user_id:
        return 0
    conn = get_db_connection()
    if not conn:
        return 0
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) as count
                FROM public.user_interactions 
                WHERE clerk_user_id = %s
            """, (clerk_user_id,))
            result = cur.fetchone()
            # Handle both dict-like and tuple-like results
            if result:
                if hasattr(result, 'get'):
                    return int(result.get('count', 0) or 0)
                else:
                    return int(result[0] or 0)
            return 0
    except Exception as e:
        logger.error(f"Error counting user interactions: {e}")
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


def count_user_interests(clerk_user_id: str) -> int:
    """Return number of rows in user_interests for a clerk_user_id."""
    if not clerk_user_id:
        return 0
    conn = get_db_connection()
    if not conn:
        return 0
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) as count
                FROM public.user_interests 
                WHERE clerk_user_id = %s
            """, (clerk_user_id,))
            result = cur.fetchone()
            # Handle both dict-like and tuple-like results
            if result:
                if hasattr(result, 'get'):  # dict-like (RealDictCursor)
                    return int(result.get('count', 0) or 0)
                else:  # tuple-like
                    return int(result[0] or 0)
            return 0
    except Exception as e:
        print(f"Error counting user interests: {e}")
        return 0
    finally:
        try:
            conn.close()
        except:
            pass

def get_content_based_recommendations_db(user_id: int, limit: int = 20):
    """Get content-based recommendations using user's genre preferences"""
    conn = get_db_connection()
    if not conn:
        return []
    try:
        with conn.cursor() as cur:
            # Get user's preferred genres from interactions
            cur.execute("""
                SELECT b.genre, COUNT(*) as interaction_count,
                       AVG(CASE 
                           WHEN ui.interaction_type = 'click' THEN 2
                           WHEN ui.interaction_type = 'view' THEN 1  
                           WHEN ui.interaction_type = 'wishlist_add' THEN 3
                           ELSE 1 END) as preference_score
                FROM user_interactions ui
                JOIN books b ON ui.book_id = b.book_id
                WHERE ui.user_id = %s 
                AND b.genre IS NOT NULL
                AND ui.interaction_type IN ('click', 'view', 'wishlist_add')
                GROUP BY b.genre
                HAVING COUNT(*) >= 1
                ORDER BY preference_score DESC, interaction_count DESC
                LIMIT 5
            """, (user_id,))
            
            user_genre_prefs = cur.fetchall()
            
            # If no interaction-based preferences, try user_interests table
            if not user_genre_prefs:
                cur.execute("SELECT genre FROM user_interests WHERE user_id = %s", (user_id,))
                user_interests = cur.fetchall()
                if user_interests:
                    # Convert to the expected format
                    user_genre_prefs = [(row[0], 1, 1.0) for row in user_interests]

            if not user_genre_prefs:
                return []

            # Get books user already interacted with
            cur.execute("SELECT book_id FROM user_interactions WHERE user_id = %s", (user_id,))
            interacted_books = [row[0] for row in cur.fetchall()]

            # Build genre conditions
            genre_conditions = " OR ".join(["LOWER(b.genre) ILIKE %s" for _ in user_genre_prefs])
            genre_params = [f"%{genre[0].lower()}%" for genre in user_genre_prefs]
            
            exclude_clause = ""
            if interacted_books:
                exclude_clause = f"AND b.book_id NOT IN ({','.join(map(str, interacted_books))})"

            query = f"""
                SELECT b.book_id AS id, b.title, b.author, b.genre,
                       b.cover_image_url AS coverurl, b.rating
                FROM books b
                WHERE ({genre_conditions})
                {exclude_clause}
                AND b.rating >= 3.0
                AND b.cover_image_url IS NOT NULL
                ORDER BY b.rating DESC NULLS LAST, b.book_id
                LIMIT %s
            """

            params = genre_params + [limit]
            cur.execute(query, params)
            return cur.fetchall()

    except Exception as e:
        print("get_content_based_recommendations_db error:", e)
        return []
    finally:
        conn.close()

def get_hybrid_recommendations_db(user_id: int, limit: int = 20):
    """Get hybrid recommendations combining collaborative and content-based filtering"""
    try:
        # Get collaborative recommendations (40% weight)
        collaborative_books = get_collaborative_recommendations_db(user_id, limit // 2)
        
        # Get content-based recommendations (40% weight)  
        content_books = get_content_based_recommendations_db(user_id, limit // 2)
        
        # Get trending books as fallback (20% weight)
        trending_result = get_trending_books_db('monthly', 1, limit // 4)
        trending_books = trending_result['books'] if trending_result else []
        
        # Combine and remove duplicates
        seen_ids = set()
        combined_books = []
        
        # Add collaborative books first (highest priority)
        for book in collaborative_books:
            if book['id'] not in seen_ids:
                book_dict = dict(book)
                book_dict['recommendation_type'] = 'collaborative'
                combined_books.append(book_dict)
                seen_ids.add(book['id'])
        
        # Add content-based books
        for book in content_books:
            if book['id'] not in seen_ids and len(combined_books) < limit:
                book_dict = dict(book)
                book_dict['recommendation_type'] = 'content_based'
                combined_books.append(book_dict)
                seen_ids.add(book['id'])
        
        # Add trending books as fallback
        for book in trending_books:
            if book['id'] not in seen_ids and len(combined_books) < limit:
                book_dict = dict(book)
                book_dict['recommendation_type'] = 'trending'
                combined_books.append(book_dict)
                seen_ids.add(book['id'])
        
        return combined_books[:limit]
        
    except Exception as e:
        print("get_hybrid_recommendations_db error:", e)
        return []

def ensure_user_interactions_table():
    """Ensure the user_interactions table exists - call this at startup"""
    return create_user_interactions_table()

def initialize_recommendation_tables():
    """Initialize all required tables for the recommendation system"""
    print("Initializing recommendation system tables...")
    success = ensure_user_interactions_table()
    if success:
        print("✅ user_interactions table ready")
    else:
        print("❌ Failed to create user_interactions table")
    return success


# Add this function to handle the interaction recording from frontend
def record_book_click(user_id: int, book_id: int, interaction_type: str = "click"):
    """Record when user clicks on a book"""
    return record_user_interaction_db(user_id, book_id, interaction_type)


# -----------------------------------------------------------------------------
# USER INTERESTS
# -----------------------------------------------------------------------------
def save_user_interests_db(clerk_user_id: str, interests: list[str]):
    """
    Save a user's interests to the user_interests table using only clerk_user_id.
    Ensures the user_interests table exists, deletes old interests, and inserts new ones.
    Returns True on success, False on error.
    """
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            # Ensure user_interests table exists with the required columns
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_interests (
                    id SERIAL PRIMARY KEY,
                    clerk_user_id TEXT NOT NULL,
                    genre VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            # Delete previous interests for this user (by clerk_user_id)
            cur.execute("DELETE FROM user_interests WHERE clerk_user_id = %s;", (clerk_user_id,))
            
            # Insert new interests
            for genre in interests:
                cur.execute(
                    "INSERT INTO user_interests (clerk_user_id, genre) VALUES (%s, %s);",
                    (clerk_user_id, genre)
                )
            
            conn.commit()
            return True
    except Exception as e:
        print("save_user_interests_db error:", e)
        conn.rollback()
        return False
    finally:
        conn.close()


# recommendations counting

def save_recommendations_db(user_id: int | None, clerk_user_id: str | None, books: list[dict], rec_type: str = "hybrid"):
    """
    Save each recommended book into the recommendations table.
    """
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            # Ensure table has clerk_user_id (if not, you can skip this part)
            cur.execute("""
                ALTER TABLE recommendations
                ADD COLUMN IF NOT EXISTS clerk_user_id TEXT;
            """)

            for book in books:
                cur.execute("""
                    INSERT INTO recommendations (user_id, clerk_user_id, book_id, recommendation_type, create_at, is_viewed)
                    VALUES (%s, %s, %s, %s, NOW(), FALSE)
                """, (
                    user_id,
                    clerk_user_id,
                    book.get("id"),
                    rec_type
                ))
        conn.commit()
        return True
    except Exception as e:
        print("save_recommendations_db error:", e)
        conn.rollback()
        return False
    finally:
        conn.close()

def count_recommendations_db(user_id: int | None = None, clerk_user_id: str | None = None) -> int:
    """
    Count total recommendations generated for a given user.
    """
    conn = get_db_connection()
    if not conn:
        return 0
    try:
        with conn.cursor() as cur:
            if clerk_user_id:
                cur.execute("SELECT COUNT(*) AS c FROM recommendations WHERE clerk_user_id = %s", (clerk_user_id,))
            elif user_id:
                cur.execute("SELECT COUNT(*) AS c FROM recommendations WHERE user_id = %s", (user_id,))
            else:
                return 0

            row = cur.fetchone()
            return row["c"] if row and "c" in row else 0
    except Exception as e:
        print("count_recommendations_db error:", e)
        return 0
    finally:
        conn.close()



