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

load_dotenv()

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
        
        with conn.cursor() as cursor:
            # Build conditions for exact word matches vs partial matches
            exact_conditions = []
            partial_conditions = []
            params = []
            
            for word in query_words:
                # Exact word match using word boundaries
                word_boundary = f'\\m{word}\\M'  # PostgreSQL word boundaries
                word_pattern = f"%{word}%"
                
                # Exact word conditions (higher priority)
                exact_conditions.append("(b.title ~* %s OR b.author ~* %s OR b.genre ~* %s)")
                params.extend([word_boundary, word_boundary, word_boundary])
                
                # Partial match conditions (fallback)
                partial_conditions.append("(b.title ILIKE %s OR b.author ILIKE %s OR b.genre ILIKE %s)")
                params.extend([word_pattern, word_pattern, word_pattern])
            
            # Combine conditions: prefer exact matches, allow partial as fallback
            exact_clause = " AND ".join(exact_conditions)
            partial_clause = " AND ".join(partial_conditions)
            
            sql = f"""
                WITH exact_matches AS (
                    SELECT DISTINCT
                        b.book_id AS id,
                        b.title,
                        b.cover_image_url AS coverurl,
                        COALESCE(b.author, 'Unknown Author') AS author,
                        b.rating,
                        1 as match_type
                    FROM books b
                    WHERE {exact_clause}
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
                    AND b.book_id NOT IN (SELECT id FROM exact_matches)
                )
                SELECT id, title, coverurl, author, rating
                FROM (
                    SELECT * FROM exact_matches
                    UNION ALL
                    SELECT * FROM partial_matches
                ) combined
                ORDER BY match_type ASC, rating DESC NULLS LAST
                LIMIT 50
            """
            
            cursor.execute(sql, params)
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
                SELECT id, title, coverurl, rating, author, corrected_date AS publication_date
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

# # RECORD USER INTERACTION
# def record_user_interaction(user_id: int, book_id: int, interaction_type: str = "view"):
#     conn = get_db_connection()
#     if not conn:
#         return None
#     try:
#         with conn.cursor() as cur:
#             cur.execute("""
#                 INSERT INTO user_interactions (user_id, book_id, interaction_type)
#                 VALUES (%s, %s, %s)
#                 RETURNING id, user_id, book_id, interaction_type, timestamp
#             """, (user_id, book_id, interaction_type))
#             return cur.fetchone()
#     except Exception as e:
#         print("record_user_interaction error:", e)
#         return None
#     finally:
#         conn.close()

def record_user_interaction_db(user_id: int, book_id: int, interaction_type: str = "view", rating: float = None):
    """Enhanced user interaction recording with more interaction types"""
    conn = get_db_connection()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            # First, try to insert into the table
            cur.execute("""
                INSERT INTO user_interactions (user_id, book_id, interaction_type, rating, timestamp)
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                RETURNING id, user_id, book_id, interaction_type, timestamp
            """, (user_id, book_id, interaction_type, rating))
            result = cur.fetchone()
            conn.commit()
            return result
    except psycopg2.errors.UndefinedTable:
        # Table doesn't exist, create it
        print("user_interactions table doesn't exist, creating it...")
        conn.rollback()
        conn.close()
        
        # Create the table
        if create_user_interactions_table():
            # Retry the insert
            conn = get_db_connection()
            if conn:
                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO user_interactions (user_id, book_id, interaction_type, rating, timestamp)
                            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                            RETURNING id, user_id, book_id, interaction_type, timestamp
                        """, (user_id, book_id, interaction_type, rating))
                        result = cur.fetchone()
                        conn.commit()
                        return result
                except Exception as e:
                    print("record_user_interaction_db retry error:", e)
                    conn.rollback()
                    return None
                finally:
                    conn.close()
        return None
    except Exception as e:
        print("record_user_interaction_db error:", e)
        conn.rollback()
        return None
    finally:
        if conn and not conn.closed:
            conn.close()

def get_user_interactions_db(user_id: int, limit: int = 100):
    """Get user interaction history"""
    conn = get_db_connection()
    if not conn:
        return []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ui.book_id, ui.interaction_type, ui.rating, ui.timestamp,
                       b.title, b.author, b.genre, b.cover_image_url
                FROM user_interactions ui
                LEFT JOIN books b ON ui.book_id = b.book_id
                WHERE ui.user_id = %s
                ORDER BY ui.timestamp DESC
                LIMIT %s
            """, (user_id, limit))
            return cur.fetchall()
    except psycopg2.errors.UndefinedTable:
        print("user_interactions table doesn't exist, creating it...")
        create_user_interactions_table()
        return []  # Return empty list for now
    except Exception as e:
        print("get_user_interactions_db error:", e)
        return []
    finally:
        conn.close()

def get_collaborative_recommendations_db(user_id: int, limit: int = 20):
    """Get collaborative filtering recommendations based on similar users"""
    conn = get_db_connection()
    if not conn:
        return []
    try:
        with conn.cursor() as cur:
            # Find users with similar interaction patterns
            cur.execute("""
                WITH user_books AS (
                    SELECT book_id 
                    FROM user_interactions 
                    WHERE user_id = %s 
                    AND interaction_type IN ('click', 'view', 'wishlist_add')
                ),
                similar_users AS (
                    SELECT ui2.user_id, COUNT(*) as common_books
                    FROM user_interactions ui2
                    WHERE ui2.book_id IN (SELECT book_id FROM user_books)
                    AND ui2.user_id != %s
                    AND ui2.interaction_type IN ('click', 'view', 'wishlist_add')
                    GROUP BY ui2.user_id
                    HAVING COUNT(*) >= 2
                    ORDER BY common_books DESC
                    LIMIT 10
                ),
                recommended_books AS (
                    SELECT b.book_id, b.title, b.author, b.genre, b.cover_image_url, 
                           b.rating, COUNT(*) as recommendation_score
                    FROM user_interactions ui
                    JOIN books b ON ui.book_id = b.book_id
                    WHERE ui.user_id IN (SELECT user_id FROM similar_users)
                    AND ui.book_id NOT IN (SELECT book_id FROM user_books)
                    AND ui.interaction_type IN ('click', 'view', 'wishlist_add')
                    AND b.rating >= 3.0
                    GROUP BY b.book_id, b.title, b.author, b.genre, b.cover_image_url, b.rating
                    ORDER BY recommendation_score DESC, b.rating DESC
                    LIMIT %s
                )
                SELECT book_id AS id, title, author, genre, cover_image_url AS coverurl, rating
                FROM recommended_books
            """, (user_id, user_id, limit))
            return cur.fetchall()
    except psycopg2.errors.UndefinedTable:
        print("user_interactions table doesn't exist for collaborative filtering, creating it...")
        create_user_interactions_table()
        return []  # Return empty list for now
    except Exception as e:
        print("get_collaborative_recommendations_db error:", e)
        return []
    finally:
        conn.close()

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

def get_content_based_recommendations_db(user_id: int, limit: int = 20):
    """Get content-based recommendations using user's genre preferences"""
    conn = get_db_connection()
    if not conn:
        return []
    try:
        with conn.cursor() as cur:
            # Get user's preferred genres
            user_genres = get_user_genre_preferences_db(user_id)
            if not user_genres:
                # Fallback to user_interests table
                cur.execute("SELECT genre FROM user_interests WHERE user_id = %s", (user_id,))
                user_interests = cur.fetchall()
                if user_interests:
                    genre_conditions = " OR ".join(["LOWER(b.genre) ILIKE %s" for _ in user_interests])
                    genre_params = [f"%{genre[0].lower()}%" for genre in user_interests]
                else:
                    return []
            else:
                genre_conditions = " OR ".join(["LOWER(b.genre) ILIKE %s" for _ in user_genres])
                genre_params = [f"%{genre[0].lower()}%" for genre in user_genres]
            
            # Get books user has already interacted with
            try:
                cur.execute("""
                    SELECT book_id FROM user_interactions 
                    WHERE user_id = %s
                """, (user_id,))
                interacted_books = [row[0] for row in cur.fetchall()]
            except psycopg2.errors.UndefinedTable:
                # Table doesn't exist yet, no books to exclude
                interacted_books = []
            
            exclude_clause = ""
            if interacted_books:
                exclude_clause = f"AND b.book_id NOT IN ({','.join(map(str, interacted_books))})"
            
            # Get recommendations based on preferred genres
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
                book['recommendation_type'] = 'collaborative'
                combined_books.append(book)
                seen_ids.add(book['id'])
        
        # Add content-based books
        for book in content_books:
            if book['id'] not in seen_ids and len(combined_books) < limit:
                book['recommendation_type'] = 'content_based'
                combined_books.append(book)
                seen_ids.add(book['id'])
        
        # Add trending books as fallback
        for book in trending_books:
            if book['id'] not in seen_ids and len(combined_books) < limit:
                book['recommendation_type'] = 'trending'
                combined_books.append(book)
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
def save_user_interests_db(user_id: int, clerk_user_id: str, interests: list[str]):
    """
    Save a user's interests to the user_interests table.
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
                    user_id INTEGER NOT NULL,
                    clerk_user_id TEXT NOT NULL,
                    genre VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            # Delete previous interests for this user (by user_id)
            cur.execute("DELETE FROM user_interests WHERE user_id = %s;", (user_id,))
            # Insert new interests (user_id, clerk_user_id, genre)
            for genre in interests:
                cur.execute(
                    "INSERT INTO user_interests (user_id, clerk_user_id, genre) VALUES (%s, %s, %s);",
                    (user_id, clerk_user_id, genre)
                )
            conn.commit()
            return True
    except Exception as e:
        print("save_user_interests_db error:", e)
        conn.rollback()
        return False
    finally:
        conn.close()
