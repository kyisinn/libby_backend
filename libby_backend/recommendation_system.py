# Enhanced Book Recommendation System - Database Integrated Version
# Provides accurate recommendations from actual book database

import sqlite3
import json
import os
import random
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict, Counter
import logging
import os
import random
import math
from decimal import Decimal

# Import your existing database functions
from libby_backend.database import (
    get_db_connection, search_books_db, get_trending_books_db, 
    get_books_by_major_db, get_book_by_id_db, get_books_by_genre_db
)
from libby_backend.database import (
    record_user_interaction_db,
    collaborative_filtering_recommendations_pg,
    count_user_interactions,
    count_user_interests,
)
from psycopg2.extras import RealDictCursor

def safe_float_conversion(value):
    """
    Safely convert a value to float, handling Decimal objects and None values.
    
    Args:
        value: The value to convert (can be Decimal, float, int, str, or None)
        
    Returns:
        float or None: The converted float value or None if conversion fails
    """
    if value is None:
        return None
    
    try:
        if isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, (int, float)):
            return float(value)
        elif isinstance(value, str):
            if value.strip():
                return float(value)
            return None
        else:
            return float(value)
    except (TypeError, ValueError, AttributeError):
        return None

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Utility functions for enhanced functionality
def serialize_for_cache(obj):
    """Safely serialize objects for caching"""
    if hasattr(obj, 'to_dict'):
        return obj.to_dict()
    elif isinstance(obj, list):
        return [serialize_for_cache(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: serialize_for_cache(value) for key, value in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, (datetime, timedelta)):
        return obj.isoformat()
    else:
        return obj

@dataclass
class Book:
    """Book data structure matching your database schema"""
    id: int
    title: str
    author: str
    genre: Optional[str] = None
    description: Optional[str] = None
    cover_image_url: Optional[str] = None
    rating: Optional[float] = None
    publication_date: Optional[str] = None
    pages: Optional[int] = None
    language: Optional[str] = None
    isbn: Optional[str] = None
    similarity_score: Optional[float] = None

    def __post_init__(self):
        # Ensure rating is always a float or None for JSON serialization
        if self.rating is not None:
            self.rating = safe_float_conversion(self.rating)

    def to_dict(self):
        """Convert to dictionary for JSON serialization"""
        # Ensure we always have a cover image URL
        cover_url = self.cover_image_url or f"https://placehold.co/320x480/1e1f22/ffffff?text={self.title}"
        
        return {
            'id': self.id,
            'title': self.title,
            'author': self.author,
            'genre': self.genre,
            'description': self.description,
            'cover_image_url': cover_url,
            'coverurl': cover_url,  # Frontend compatibility
            'rating': safe_float_conversion(self.rating),
            'publication_date': self.publication_date,
            'pages': self.pages,
            'language': self.language,
            'isbn': self.isbn,
            'similarity_score': safe_float_conversion(getattr(self, 'similarity_score', None))
        }

@dataclass
class UserProfile:
    """User profile with preferences and interaction history"""
    user_id: str
    email: str
    selected_genres: List[str]
    clerk_user_id: Optional[str] = None
    reading_history: List[int] = None
    wishlist: List[int] = None
    search_history: List[str] = None
    interaction_weights: Dict[str, float] = None
    favorite_authors: List[str] = None
    preferred_rating_threshold: float = 3.0
    last_recommendation_update: Optional[datetime] = None
    
    def __post_init__(self):
        if self.reading_history is None:
            self.reading_history = []
        if self.wishlist is None:
            self.wishlist = []
        if self.search_history is None:
            self.search_history = []
        if self.interaction_weights is None:
            self.interaction_weights = {}
        if self.favorite_authors is None:
            self.favorite_authors = []

@dataclass
class RecommendationResult:
    """Container for recommendation results"""
    books: List[Book]
    algorithm_used: str
    confidence_score: float
    reasons: List[str]
    generated_at: datetime
    # Optional telemetry / diagnostics
    contributions: Optional[Dict[str, int]] = None
    interaction_count: Optional[int] = 0

# Enhanced utility functions for better functionality
def get_books_by_advanced_genre_search_enhanced(target_genre: str, limit: int = 20, exclude_ids: List[int] = None) -> List[Book]:
    """Advanced genre search with better error handling and database compatibility"""
    try:
        if exclude_ids is None:
            exclude_ids = []

        conn = get_db_connection()
        if not conn:
            logger.error("No database connection available")
            return []

        books = []

        with conn.cursor() as cursor:
            # Use correct column name: book_id instead of id
            exclude_clause = ""
            params = [f'%{target_genre}%']

            if exclude_ids:
                placeholders = ','.join(['%s'] * len(exclude_ids))
                exclude_clause = f"AND book_id NOT IN ({placeholders})"
                params.extend(exclude_ids)

            params.append(limit)

            query = f"""
                SELECT book_id, title, author, genre, description, cover_image_url, 
                       rating, publication_date, pages, language, isbn
                FROM books 
                WHERE genre ILIKE %s {exclude_clause}
                AND title IS NOT NULL 
                AND author IS NOT NULL
                ORDER BY rating DESC NULLS LAST, book_id
                LIMIT %s
            """

            cursor.execute(query, params)
            rows = cursor.fetchall()

            for row in rows:
                try:
                    book = Book(
                        id=row[0],  # book_id is first column
                        title=row[1] or '',
                        author=row[2] or 'Unknown Author',
                        genre=row[3],
                        description=row[4],
                        cover_image_url=row[5],
                        rating=safe_float_conversion(row[6]),
                        publication_date=str(row[7]) if row[7] else None,
                        pages=row[8],
                        language=row[9],
                        isbn=row[10]
                    )
                    books.append(book)
                except Exception as e:
                    logger.error(f"Error creating book object: {e}")
                    continue

        conn.close()
        logger.info(f"Advanced genre search for '{target_genre}' returned {len(books)} books")
        return books

    except Exception as e:
        logger.error(f"Error in advanced genre search for '{target_genre}': {e}")
        return []

def get_quality_trending_books_fallback(limit: int, exclude_ids: List[int] = None) -> List[Book]:
    """Fallback function to get trending books when other methods fail"""
    try:
        if exclude_ids is None:
            exclude_ids = []
            
        conn = get_db_connection()
        if not conn:
            return []
            
        books = []
        with conn.cursor() as cursor:
            exclude_clause = ""
            params = []
            
            if exclude_ids:
                placeholders = ','.join(['%s'] * len(exclude_ids))
                exclude_clause = f"AND book_id NOT IN ({placeholders})"
                params.extend(exclude_ids)
            
            params.append(limit)
            
            cursor.execute(f"""
                SELECT book_id, title, author, genre, description, cover_image_url, 
                       rating, publication_date, pages, language, isbn
                FROM books 
                WHERE rating IS NOT NULL 
                AND rating >= 3.0
                AND cover_image_url IS NOT NULL
                {exclude_clause}
                ORDER BY rating DESC, book_id
                LIMIT %s
            """, params)
            
            for row in cursor.fetchall():
                try:
                    book = Book(
                        id=row[0],
                        title=row[1] or '',
                        author=row[2] or 'Unknown Author', 
                        genre=row[3],
                        description=row[4],
                        cover_image_url=row[5],
                        rating=safe_float_conversion(row[6]),
                        publication_date=str(row[7]) if row[7] else None,
                        pages=row[8],
                        language=row[9],
                        isbn=row[10]
                    )
                    book.similarity_score = 0.6  # Base trending score
                    books.append(book)
                except Exception as e:
                    logger.error(f"Error creating book from row: {e}")
                    continue
                    
        conn.close()
        return books
        
    except Exception as e:
        logger.error(f"Error in fallback trending books: {e}")
        return []

def get_diversity_books_enhanced(profile, limit: int, exclude_ids: List[int]) -> List[Book]:
    """Get diverse books with zero division protection"""
    try:
        if limit <= 0:
            return []
            
        # Get available genres from database
        conn = get_db_connection()
        if not conn:
            return []
            
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT LOWER(genre) as genre_lower
                FROM books 
                WHERE genre IS NOT NULL 
                AND genre != ''
                LIMIT 50
            """)
            available_genres = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        if not available_genres:
            logger.warning("No genres found in database")
            return []
        
        # Ensure exclude_ids is a list
        if exclude_ids is None:
            exclude_ids = []

        # Find unexplored genres
        user_genres_lower = {genre.lower() for genre in (profile.selected_genres or [])}
        interaction_genres = set(profile.interaction_weights.keys() if profile.interaction_weights else [])
        explored_genres = user_genres_lower.union(interaction_genres)
        
        unexplored_genres = [
            genre for genre in available_genres 
            if genre not in explored_genres
        ]
        
        if not unexplored_genres:
            # If no unexplored genres, use random selection from all
            unexplored_genres = random.sample(available_genres, min(3, len(available_genres)))
        
        # CRITICAL: Prevent zero division
        selected_genres = random.sample(unexplored_genres, min(3, len(unexplored_genres)))
        
        if not selected_genres:
            logger.warning("No genres selected for diversity")
            return []
        
        diversity_books = []
        books_per_genre = max(1, limit // max(1, len(selected_genres)))  # This is now safe

        for genre in selected_genres:
            genre_books = get_books_by_advanced_genre_search_enhanced(genre, books_per_genre, exclude_ids)
            for book in genre_books:
                if book.id not in exclude_ids:
                    book.similarity_score = 0.4  # Lower score for diversity
                    diversity_books.append(book)
                    exclude_ids.append(book.id)
                    
            if len(diversity_books) >= limit:
                break
        
        return diversity_books[:limit]
        
    except Exception as e:
        logger.error(f"Error getting diversity books: {e}")
        return []

class EnhancedBookRecommendationEngine:
    """
    Enhanced book recommendation engine with accurate database integration
    """
    
    def __init__(self):
        self.db_path = os.getenv('RECOMMENDATION_DB_PATH', 'recommendations.db')
        
        # Enhanced algorithm weights
        self.weights = {
            'content_based': 0.35,
            'collaborative': 0.25,
            'trending': 0.20,
            'author_based': 0.15,
            'diversity': 0.05
        }
        
        # Genre mappings for better matching
        self.genre_synonyms = {
            'science fiction': ['sci-fi', 'scifi', 'fantasy', 'futuristic'],
            'fantasy': ['magic', 'medieval', 'dragons', 'mythology'],
            'mystery': ['crime', 'detective', 'thriller', 'suspense'],
            'romance': ['love', 'romantic', 'relationships'],
            'biography': ['autobiography', 'memoir', 'life story'],
            'history': ['historical', 'war', 'politics', 'ancient'],
            'business': ['economics', 'finance', 'management', 'entrepreneurship'],
            'psychology': ['mental health', 'behavior', 'cognitive'],
            'technology': ['computer', 'programming', 'ai', 'digital'],
            'self-help': ['personal development', 'motivation', 'productivity']
        }
        
        self._init_database()
    
    def _init_database(self):
        """Initialize SQLite database for storing recommendation data"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # User interactions table (enhanced)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_interactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    book_id INTEGER NOT NULL,
                    interaction_type TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    weight REAL DEFAULT 1.0,
                    rating REAL DEFAULT NULL
                )
            ''')
            
            # User preferences table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_preferences (
                    user_id TEXT PRIMARY KEY,
                    preferred_genres TEXT,
                    favorite_authors TEXT,
                    rating_threshold REAL DEFAULT 3.0,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Book similarity cache
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS book_similarities (
                    book1_id INTEGER,
                    book2_id INTEGER,
                    similarity_score REAL,
                    last_calculated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (book1_id, book2_id)
                )
            ''')
            
            # Book cache table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS book_cache (
                    book_id INTEGER PRIMARY KEY,
                    data TEXT NOT NULL, -- JSON data
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # User recommendations cache
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_recommendations (
                    user_id TEXT PRIMARY KEY,
                    recommendations TEXT NOT NULL, -- JSON data
                    algorithm_used TEXT NOT NULL,
                    generated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    expires_at DATETIME NOT NULL
                )
            ''')
            
            # Create indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_interactions ON user_interactions(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_book_interactions ON user_interactions(book_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_similarities ON book_similarities(book1_id)')
            
            conn.commit()
            conn.close()
            
            logger.info("Enhanced recommendation database initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing enhanced recommendation database: {e}")
    
    def get_all_books_with_metadata(self, limit: int = 1000) -> List[Book]:
        """Get all books from the main database with full metadata"""
        try:
            conn = get_db_connection()
            if not conn:
                return []

            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT book_id, title, author, genre, description, cover_image_url,
                           rating, publication_date, pages, language, isbn
                    FROM books
                    WHERE title IS NOT NULL AND author IS NOT NULL
                    ORDER BY rating DESC NULLS LAST, book_id
                    LIMIT %s
                """, (limit,))
                rows = cursor.fetchall()

            books = []
            for row in rows:
                try:
                    books.append(Book(
                        id=row["book_id"],
                        title=row.get("title") or "",
                        author=row.get("author") or "Unknown Author",
                        genre=row.get("genre"),
                        description=row.get("description"),
                        cover_image_url=row.get("cover_image_url"),
                        rating=safe_float_conversion(row.get("rating")),
                        publication_date=str(row.get("publication_date")) if row.get("publication_date") else None,
                        pages=row.get("pages"),
                        language=row.get("language"),
                        isbn=row.get("isbn")
                    ))
                except Exception:
                    # Skip malformed rows but continue processing
                    continue

            conn.close()
            return books

        except Exception as e:
            logger.error(f"Error getting all books: {e}")
            return []
    
    def get_books_by_advanced_genre_search(self, target_genre: str, limit: int = 20, exclude_ids: List[int] = None) -> List[Book]:
        """Advanced genre search using synonyms and partial matching (DB-safe)."""
        try:
            if exclude_ids is None:
                exclude_ids = []

            search_terms = [target_genre.lower()]
            for main_genre, synonyms in self.genre_synonyms.items():
                if target_genre.lower() in main_genre or main_genre in target_genre.lower():
                    search_terms.extend(synonyms)

            conn = get_db_connection()
            if not conn:
                logger.error("No database connection available for advanced genre search")
                return []

            books: List[Book] = []
            # use RealDictCursor so row access by keys is safe
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                for term in set(search_terms):
                    params = [f"%{term}%"]
                    exclude_clause = ""
                    if exclude_ids:
                        placeholders = ",".join(["%s"] * len(exclude_ids))
                        exclude_clause = f"AND book_id NOT IN ({placeholders})"
                        params.extend(exclude_ids)
                    params.append(limit)

                    query = f"""
                        SELECT book_id, title, author, genre, description, cover_image_url,
                               rating, publication_date, pages, language, isbn
                        FROM books
                        WHERE LOWER(genre) LIKE %s {exclude_clause}
                          AND title IS NOT NULL
                          AND author IS NOT NULL
                        ORDER BY rating DESC NULLS LAST, book_id
                        LIMIT %s
                    """
                    cursor.execute(query, params)
                    rows = cursor.fetchall()

                    for row in rows:
                        book_id = row.get("book_id")
                        if not book_id:
                            continue
                        if book_id in exclude_ids:
                            continue

                        books.append(Book(
                            id=book_id,
                            title=row.get("title") or "",
                            author=row.get("author") or "Unknown Author",
                            genre=row.get("genre"),
                            description=row.get("description"),
                            cover_image_url=row.get("cover_image_url"),
                            rating=safe_float_conversion(row.get("rating")),
                            publication_date=str(row.get("publication_date")) if row.get("publication_date") else None,
                            pages=row.get("pages"),
                            language=row.get("language"),
                            isbn=row.get("isbn")
                        ))
                        exclude_ids.append(book_id)

                    if len(books) >= limit:
                        break

            conn.close()
            logger.info(f"Advanced genre search for '{target_genre}' returned {len(books[:limit])} books")
            return books[:limit]

        except Exception as e:
            logger.error(f"Error in advanced genre search: {e}")
            return []
    
    def get_books_by_author(self, author_name: str, limit: int = 10, exclude_ids: List[int] = None) -> List[Book]:
        """Get books by a specific author"""
        try:
            exclude_ids = exclude_ids or []
            conn = get_db_connection()
            if not conn:
                return []

            books = []
            with conn.cursor() as cursor:
                params = [f'%{author_name.lower()}%']
                exclude_clause = ""
                if exclude_ids:
                    placeholders = ",".join(["%s"] * len(exclude_ids))
                    exclude_clause = f"AND book_id NOT IN ({placeholders})"
                    params.extend(exclude_ids)
                params.append(limit)

                cursor.execute(f"""
                    SELECT book_id, title, author, genre, description, cover_image_url,
                           rating, publication_date, pages, language, isbn
                    FROM books
                    WHERE LOWER(author) LIKE %s {exclude_clause}
                    ORDER BY rating DESC NULLS LAST
                    LIMIT %s
                """, params)

                for row in cursor.fetchall():
                    book = Book(
                        id=row[0], title=row[1], author=row[2], genre=row[3],
                        description=row[4], cover_image_url=row[5], rating=row[6],
                        publication_date=str(row[7]) if row[7] else None,
                        pages=row[8], language=row[9], isbn=row[10]
                    )
                    books.append(book)

            conn.close()
            return books

        except Exception as e:
            logger.error(f"Error getting books by author: {e}")
            return []
    
    # def record_user_interaction(self, user_id: str, book_id: int, interaction_type: str, weight: float = 1.0, rating: float = None):
    #     """Enhanced interaction recording with rating support"""
    #     try:
    #         conn = sqlite3.connect(self.db_path)
    #         cursor = conn.cursor()
            
    #         cursor.execute('''
    #             INSERT INTO user_interactions (user_id, book_id, interaction_type, weight, rating)
    #             VALUES (?, ?, ?, ?, ?)
    #         ''', (user_id, book_id, interaction_type, weight, rating))
            
    #         conn.commit()
    #         conn.close()
            
    #         # Update user preferences based on interaction
    #         self._update_user_preferences(user_id, book_id, interaction_type, weight)
            
    #         logger.info(f"Enhanced interaction recorded: {user_id} -> {book_id} ({interaction_type}, weight: {weight})")
            
    #     except Exception as e:
    #         logger.error(f"Error recording enhanced interaction: {e}")

    def record_interaction(self, user_id: Optional[int], book_id: int, interaction_type: str, rating: float = None, clerk_user_id: Optional[str] = None) -> Dict:
        """
        API shim: write interaction to Postgres via `record_user_interaction_db`.
        Accepts either numeric `user_id` or `clerk_user_id` and returns a simple dict.
        """
        try:
            saved = record_user_interaction_db(
                user_id=user_id,
                clerk_user_id=clerk_user_id,
                book_id=book_id,
                interaction_type=interaction_type,
                rating=rating
            )
            ok = saved is not None
            return {
                "success": ok,
                "message": f"{'Saved' if ok else 'Failed to save'} interaction",
                "record": saved
            }
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def _update_user_preferences(self, user_id: str, book_id: int, interaction_type: str, weight: float):
        """Update user preferences based on interactions"""
        try:
            # Get book details
            book_data = get_book_by_id_db(book_id)
            if not book_data:
                return
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get current preferences
            cursor.execute('''
                SELECT preferred_genres, favorite_authors, rating_threshold
                FROM user_preferences
                WHERE user_id = ?
            ''', (user_id,))
            
            current = cursor.fetchone()
            preferred_genres = []
            favorite_authors = []
            rating_threshold = 3.0
            
            if current:
                preferred_genres = json.loads(current[0]) if current[0] else []
                favorite_authors = json.loads(current[1]) if current[1] else []
                rating_threshold = current[2] or 3.0
            
            # Update based on positive interactions
            if interaction_type in ['wishlist_add', 'like'] and weight > 0:
                if book_data.get('genre') and book_data['genre'] not in preferred_genres:
                    preferred_genres.append(book_data['genre'])
                if book_data.get('author') and book_data['author'] not in favorite_authors:
                    favorite_authors.append(book_data['author'])
            
            # Save updated preferences
            cursor.execute('''
                INSERT OR REPLACE INTO user_preferences 
                (user_id, preferred_genres, favorite_authors, rating_threshold)
                VALUES (?, ?, ?, ?)
            ''', (user_id, json.dumps(preferred_genres), json.dumps(favorite_authors), rating_threshold))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error updating user preferences: {e}")
    
    def get_user_profile_enhanced(self, user_id: str, email: str = None, selected_genres: List[str] = None) -> UserProfile:
        """Enhanced user profile building with author preferences and rating thresholds"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get user interactions with ratings
            cursor.execute('''
                SELECT book_id, interaction_type, timestamp, rating
                FROM user_interactions
                WHERE user_id = ?
                ORDER BY timestamp DESC
            ''', (user_id,))
            
            interactions = cursor.fetchall()
            
            # Get stored preferences
            cursor.execute('''
                SELECT preferred_genres, favorite_authors, rating_threshold
                FROM user_preferences
                WHERE user_id = ?
            ''', (user_id,))
            
            prefs = cursor.fetchone()
            conn.close()
            
            # Build enhanced profile from interactions
            reading_history = []
            wishlist = []
            interaction_weights = defaultdict(float)
            favorite_authors = []
            rating_threshold = 3.0
            
            if prefs:
                favorite_authors = json.loads(prefs[1]) if prefs[1] else []
                rating_threshold = prefs[2] or 3.0
            
            for book_id, interaction_type, weight, timestamp, rating in interactions:
                if interaction_type == 'view':
                    reading_history.append(book_id)
                elif interaction_type in ['wishlist_add', 'wishlist']:
                    wishlist.append(book_id)
                
                # Get book genre and author for weight calculation
                book = self._get_book_from_main_db(book_id)
                if book:
                    if book.genre:
                        interaction_weights[book.genre.lower()] += weight
                    
                    # Track favorite authors based on positive interactions
                    if book.author and interaction_type in ['wishlist_add', 'like'] and book.author not in favorite_authors:
                        favorite_authors.append(book.author)
            
            # treat non-numeric user_id as a Clerk ID
            clerk_id = None if str(user_id).isdigit() else str(user_id)
            profile = UserProfile(
                user_id=user_id,
                email=email or f"user_{user_id}@example.com",
                selected_genres=selected_genres or [],
                clerk_user_id=clerk_id,
                reading_history=reading_history,
                wishlist=wishlist,
                interaction_weights=dict(interaction_weights),
                favorite_authors=favorite_authors,
                preferred_rating_threshold=rating_threshold
            )
            
            return profile
            
        except Exception as e:
            logger.error(f"Error getting enhanced user profile: {e}")
            return UserProfile(
                user_id=user_id,
                email=email or f"user_{user_id}@libby.com",
                selected_genres=selected_genres or [],
                favorite_authors=[],
                preferred_rating_threshold=3.0
            )
    
    def get_user_profile(self, user_id: str, email: str = None, selected_genres: List[str] = None) -> UserProfile:
        """Build user profile from interactions and preferences"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get user interactions
            cursor.execute('''
                SELECT book_id, interaction_type, weight, timestamp
                FROM user_interactions
                WHERE user_id = ?
                ORDER BY timestamp DESC
            ''', (user_id,))
            
            interactions = cursor.fetchall()
            conn.close()
            
            # Build profile from interactions
            reading_history = []
            wishlist = []
            interaction_weights = defaultdict(float)
            
            for book_id, interaction_type, weight, timestamp in interactions:
                if interaction_type == 'view':
                    reading_history.append(book_id)
                elif interaction_type == 'wishlist':
                    wishlist.append(book_id)
                
                # Get book genre for weight calculation using main database
                book = self._get_book_from_main_db(book_id)
                if book and book.genre:
                    interaction_weights[book.genre.lower()] += weight
            
            clerk_id = None if str(user_id).isdigit() else str(user_id)
            profile = UserProfile(
                user_id=user_id,
                email=email or f"user_{user_id}@example.com",
                selected_genres=selected_genres or [],
                clerk_user_id=clerk_id,
                reading_history=reading_history,
                wishlist=wishlist,
                interaction_weights=dict(interaction_weights)
            )
            
            return profile
            
        except Exception as e:
            logger.error(f"Error getting user profile: {e}")
            return UserProfile(user_id=user_id, email=email or f"user_{user_id}@libby.com", selected_genres=selected_genres or [])
    
    def _get_book_from_main_db(self, book_id: int) -> Optional[Book]:
        """Get book from main PostgreSQL database using existing functions"""
        try:
            book_data = get_book_by_id_db(book_id)
            if book_data:
                return Book(
                    id=book_data.get('book_id', book_id),
                    title=book_data.get('title', ''),
                    author=book_data.get('author', ''),
                    genre=book_data.get('genre'),
                    description=book_data.get('description'),
                    cover_image_url=book_data.get('cover_image_url'),
                    rating=book_data.get('rating'),
                    publication_date=book_data.get('publication_date'),
                    pages=book_data.get('pages'),
                    language=book_data.get('language'),
                    isbn=book_data.get('isbn')
                )
            return None
            
        except Exception as e:
            logger.error(f"Error getting book {book_id} from main database: {e}")
            return None
    
    def _fetch_books_by_genre(self, genre: str, limit: int = 20, exclude_ids: List[int] = None) -> List[Book]:
        """Fetch books by genre using existing database functions"""
        try:
            # Use the existing get_books_by_major_db function which searches by genre
            result = get_books_by_major_db(genre, 1, limit * 2)  # Get more than needed for filtering
            
            if result and result.get('books'):
                books = []
                exclude_ids = exclude_ids or []
                
                for book_data in result['books']:
                    book_id = book_data.get('id')
                    if book_id and book_id not in exclude_ids:
                        book = Book(
                            id=book_id,
                            title=book_data.get('title', ''),
                            author=book_data.get('author', ''),
                            genre=genre,  # We searched by this genre
                            cover_image_url=book_data.get('coverurl'),
                            rating=book_data.get('rating')
                        )
                        books.append(book)
                        
                        if len(books) >= limit:
                            break
                
                return books
                
        except Exception as e:
            logger.error(f"Error fetching books for genre {genre}: {e}")
            
        return []

    def _make_book_from_row(self, row: Dict) -> Optional[Book]:
        """Convert a dict-like DB row into a Book object."""
        try:
            book_id = row.get('id') or row.get('book_id') or row.get('bookid')
            if not book_id:
                return None
            return Book(
                id=book_id,
                title=row.get('title') or '',
                author=row.get('author') or 'Unknown Author',
                genre=row.get('genre'),
                cover_image_url=row.get('coverurl') or row.get('cover_image_url'),
                rating=safe_float_conversion(row.get('rating'))
            )
        except Exception as e:
            logger.debug(f"_make_book_from_row error: {e}")
            return None
    
    def _fetch_trending_books(self, limit: int = 20, exclude_ids: List[int] = None) -> List[Book]:
        """Fetch trending books using existing database functions"""
        try:
            result = get_trending_books_db('5years', 1, limit * 2)  # Get more for filtering
            
            if result and result.get('books'):
                books = []
                exclude_ids = exclude_ids or []
                
                for book_data in result['books']:
                    book_id = book_data.get('id')
                    if book_id and book_id not in exclude_ids:
                        book = Book(
                            id=book_id,
                            title=book_data.get('title', ''),
                            author=book_data.get('author', ''),
                            cover_image_url=book_data.get('coverurl'),
                            rating=book_data.get('rating')
                        )
                        books.append(book)
                        
                        if len(books) >= limit:
                            break
                
                return books
                
        except Exception as e:
            logger.error(f"Error fetching trending books: {e}")
            
        return []
    
    def _search_books_by_query(self, query: str, limit: int = 20, exclude_ids: List[int] = None) -> List[Book]:
        """Search books using existing search function"""
        try:
            results = search_books_db(query)
            
            if results:
                books = []
                exclude_ids = exclude_ids or []
                
                for book_data in results[:limit * 2]:  # Get more for filtering
                    book_id = book_data.get('id')
                    if book_id and book_id not in exclude_ids:
                        book = Book(
                            id=book_id,
                            title=book_data.get('title', ''),
                            author=book_data.get('author', ''),
                            cover_image_url=book_data.get('coverurl'),
                            rating=book_data.get('rating')
                        )
                        books.append(book)
                        
                        if len(books) >= limit:
                            break
                
                return books
                
        except Exception as e:
            logger.error(f"Error searching books with query {query}: {e}")
            
        return []
    
    def content_based_recommendations(self, profile: UserProfile, limit: int = 10) -> Tuple[List[Book], List[str]]:
        """Generate recommendations based on user's genre preferences and interaction history"""
        recommendations = []
        reasons = []
        exclude_ids = profile.reading_history + profile.wishlist
        
        try:
            # Weight genres based on user preferences and interactions
            genre_scores = {}
            
            # Base scores from selected genres
            for genre in profile.selected_genres:
                genre_scores[genre.lower()] = 1.0
            
            # Boost scores from interaction history
            for genre, weight in profile.interaction_weights.items():
                genre_scores[genre] = genre_scores.get(genre, 0) + weight * 0.5
            
            # If no genres, use some defaults
            if not genre_scores:
                default_genres = ['Fiction', 'Science Fiction', 'Fantasy', 'Mystery', 'Romance']
                for genre in default_genres:
                    genre_scores[genre.lower()] = 0.5
            
            # Sort genres by score
            sorted_genres = sorted(genre_scores.items(), key=lambda x: x[1], reverse=True)
            
            # Fetch books from top genres
            books_per_genre = max(1, limit // len(sorted_genres)) if sorted_genres else limit
            
            for genre, score in sorted_genres[:5]:  # Limit to top 5 genres
                # Try different search approaches
                genre_books = []
                
                # First try genre-specific search
                genre_books.extend(self._fetch_books_by_genre(genre, books_per_genre, exclude_ids))
                
                # If not enough books, try searching by genre name
                if len(genre_books) < books_per_genre:
                    search_books = self._search_books_by_query(genre, books_per_genre - len(genre_books), exclude_ids)
                    genre_books.extend(search_books)
                
                # Score books based on various factors
                for book in genre_books:
                    content_score = self._calculate_content_score(book, profile)
                    if content_score > 0.2:  # Lower threshold for inclusion
                        book.content_score = content_score
                        recommendations.append(book)
                
                if len(recommendations) >= limit * 2:
                    break
            
            # Sort by score and take top recommendations
            recommendations.sort(key=lambda x: getattr(x, 'content_score', 0), reverse=True)
            recommendations = recommendations[:limit]
            
            if recommendations:
                top_genres = [genre for genre, _ in sorted_genres[:3]]
                reasons.append(f"Based on your interest in {', '.join(top_genres)}")
                
                if profile.interaction_weights:
                    reasons.append("Considering your reading history and interactions")
            
        except Exception as e:
            logger.error(f"Error in content-based recommendations: {e}")
        
        return recommendations, reasons
    
    def _calculate_content_score(self, book: Book, profile: UserProfile) -> float:
        """Calculate content-based score for a book"""
        score = 0.0
        
        try:
            # Genre matching
            if book.genre and profile.selected_genres:
                for user_genre in profile.selected_genres:
                    if user_genre.lower() in book.genre.lower():
                        score += 0.4
                        break
            
            # Interaction weight boost
            if book.genre and book.genre.lower() in profile.interaction_weights:
                score += profile.interaction_weights[book.genre.lower()] * 0.3
            
            # Rating boost
            if book.rating:
                score += (book.rating / 5.0) * 0.2
            
            # Recency boost for newer books
            if book.publication_date:
                try:
                    if isinstance(book.publication_date, str):
                        pub_date = datetime.fromisoformat(book.publication_date.replace('Z', '+00:00'))
                        days_since_pub = (datetime.now() - pub_date.replace(tzinfo=None)).days
                        if days_since_pub < 365:  # Books from last year
                            score += 0.1
                except:
                    pass
            
            # Penalize if already interacted with
            if book.id in profile.reading_history:
                score *= 0.5
            if book.id in profile.wishlist:
                score *= 0.3
                
        except Exception as e:
            logger.error(f"Error calculating content score: {e}")
        
        return min(score, 1.0)
    
    def collaborative_filtering_recommendations(self, profile: UserProfile, limit: int = 10) -> Tuple[List[Book], List[str]]:
        """Generate collaborative filtering recommendations using the existing PostgreSQL function."""
        try:
            rows, reasons = collaborative_filtering_recommendations_pg(
                clerk_user_id=getattr(profile, 'clerk_user_id', None),
                user_id=int(profile.user_id) if getattr(profile,'user_id',None) and str(profile.user_id).isdigit() else None,
                limit=limit
            )
            books = []
            for row in (rows or []):
                b = Book(
                    id=row.get('book_id') or row.get('id'),
                    title=row.get('title') or '',
                    author=row.get('author') or 'Unknown Author',
                    genre=row.get('genre'),
                    cover_image_url=row.get('cover_image_url') or row.get('coverurl'),
                    rating=safe_float_conversion(row.get('rating'))
                )
                # make it visible to hybrid sorter
                b.similarity_score = (row.get('score') or 0.0)
                books.append(b)
            return books, reasons or []
        except Exception as e:
            logger.error(f"Error in collaborative filtering: {e}")
            return [], []
    
    def diversity_recommendations(self, profile: UserProfile, limit: int = 5) -> Tuple[List[Book], List[str]]:
        """Add diversity by recommending books from genres user hasn't explored"""
        recommendations = []
        reasons = []
        exclude_ids = profile.reading_history + profile.wishlist
        
        try:
            # Get all available genres
            all_genres = ['Fiction', 'Science Fiction', 'Fantasy', 'Mystery', 'Romance', 
                         'Thriller', 'Biography', 'History', 'Science', 'Philosophy', 
                         'Self-Help', 'Business', 'Art', 'Technology']
            
            # Find unexplored genres
            user_genres = {genre.lower() for genre in profile.selected_genres}
            interaction_genres = set(profile.interaction_weights.keys())
            explored_genres = user_genres.union(interaction_genres)
            
            unexplored_genres = [genre for genre in all_genres 
                               if genre.lower() not in explored_genres]
            
            if unexplored_genres:
                # Pick random unexplored genres
                selected_genres = random.sample(unexplored_genres, 
                                              min(3, len(unexplored_genres)))
                
                books_per_genre = max(1, limit // len(selected_genres))
                
                for genre in selected_genres:
                    genre_books = self.get_books_by_author(genre, books_per_genre * 2, exclude_ids)
                    
                    # Prefer highly rated books for diversity recommendations
                    genre_books.sort(key=lambda x: x.rating or 0, reverse=True)
                    recommendations.extend(genre_books[:books_per_genre])
                
                if recommendations:
                    reasons.append(f"Discover new genres: {', '.join(selected_genres)}")
            
        except Exception as e:
            logger.error(f"Error in diversity recommendations: {e}")
        
        return recommendations[:limit], reasons
    
    def author_based_recommendations(self, profile: UserProfile, limit: int = 10) -> Tuple[List[Book], List[str]]:
        """Recommendations based on favorite authors"""
        recommendations = []
        reasons = []
        exclude_ids = profile.reading_history + profile.wishlist
        
        try:
            # Get books from favorite authors
            for author in profile.favorite_authors[:5]:  # Limit to top 5 authors
                author_books = self.get_books_by_author(author, limit // len(profile.favorite_authors) + 2, exclude_ids)
                
                for book in author_books:
                    if book.id not in exclude_ids:
                        book.similarity_score = 0.8  # High score for favorite authors
                        recommendations.append(book)
                        exclude_ids.append(book.id)
                        
                if recommendations:
                    reasons.append(f"Books by your favorite author {author}")
                    
                if len(recommendations) >= limit:
                    break
            
            # If we have user interactions, find similar authors
            if not recommendations and profile.interaction_weights:
                popular_genres = sorted(profile.interaction_weights.items(), key=lambda x: x[1], reverse=True)[:3]
                
                for genre, weight in popular_genres:
                    genre_books = self.get_books_by_advanced_genre_search(genre, 10, exclude_ids)
                    authors_found = set()
                    
                    for book in genre_books:
                        if book.author not in authors_found and book.id not in exclude_ids:
                            book.similarity_score = 0.6 * weight
                            recommendations.append(book)
                            authors_found.add(book.author)
                            exclude_ids.append(book.id)
                            
                            if len(recommendations) >= limit:
                                break
                                
                    if recommendations:
                        reasons.append(f"New authors in your preferred genre: {genre}")
                        
                    if len(recommendations) >= limit:
                        break
                        
        except Exception as e:
            logger.error(f"Error in author-based recommendations: {e}")
        
        return recommendations[:limit], reasons
    
    def hybrid_recommendations_enhanced(self, profile: UserProfile, total_limit: int = 30) -> RecommendationResult:
        """Enhanced hybrid recommendations with better book data integration"""
        try:
            # ---------- hydrate from DB (keep yours)
            try:
                self._hydrate_profile_from_db(profile)
            except Exception:
                pass

            all_reasons = []
            # collect per-source results so we can fuse/credit correctly
            SOURCE_BOOST = {"content": 0.6, "author": 0.4, "collab": 1.0, "trending": 0.25, "diversity": 0.15}
            per_source = {}

            # ---------- content (35%)
            content_limit = int(total_limit * self.weights['content_based'])
            content_books, content_reasons = self.content_based_recommendations_enhanced(profile, content_limit)
            per_source["content"] = content_books; all_reasons += content_reasons

            # ---------- author (15%)
            author_limit = int(total_limit * self.weights['author_based'])
            author_books, author_reasons = self.author_based_recommendations(profile, author_limit)
            # leave dedupe for the fusion stage
            per_source["author"] = author_books; all_reasons += author_reasons

            # ---------- collaborative (25%)
            collab_limit = int(total_limit * self.weights['collaborative'])
            collab_books, collab_reasons = self.collaborative_filtering_recommendations(profile, collab_limit)
            per_source["collab"] = collab_books; all_reasons += collab_reasons

            # ---------- trending (20%)
            already_ids = [b.id for src in ("content","author","collab") for b in per_source.get(src, [])]
            exclude_ids = (already_ids or []) + (profile.reading_history or []) + (profile.wishlist or [])
            trending_limit = int(total_limit * self.weights['trending'])
            trending_books = self.get_quality_trending_books(trending_limit, exclude_ids, profile)
            per_source["trending"] = trending_books
            if trending_books: all_reasons.append("High-rated trending books matching your preferences")

            # ---------- diversity (5%)
            diversity_limit = int(total_limit * self.weights['diversity'])
            diversity_books = self.get_diversity_books(profile, diversity_limit, [b.id for b in trending_books] + already_ids)
            per_source["diversity"] = diversity_books
            if diversity_books: all_reasons.append("Diverse books from unexplored genres")

            # ---------- fuse, score, dedupe
            def _canon_id(v):
                try: return int(v)
                except Exception: return str(v)

            bookmap = {}  # bid -> dict(book, score, srcs:set)
            for src, books in per_source.items():
                for b in books:
                    # normalize missing scores so no source is handicapped
                    if not hasattr(b, "similarity_score") or b.similarity_score is None:
                        b.similarity_score = 0.01
                    bid = _canon_id(b.id)
                    entry = bookmap.get(bid)
                    if not entry:
                        bookmap[bid] = {"book": b, "score": float(b.similarity_score) + SOURCE_BOOST.get(src, 0.0), "srcs": {src}}
                    else:
                        # keep best intrinsic score, add boost for additional source
                        entry["score"] = max(entry["score"], float(b.similarity_score))
                        entry["score"] += SOURCE_BOOST.get(src, 0.0)
                        entry["srcs"].add(src)

            # slight guaranteed visibility for CF while dataset is small
            if per_source.get("collab"):
                for b in sorted(per_source["collab"], key=lambda x: getattr(x, "similarity_score", 0), reverse=True)[:max(2, int(total_limit*0.2))]:
                    bid = _canon_id(b.id)
                    if bid in bookmap:
                        bookmap[bid]["score"] += 0.5

            # rank by fused score
            ranked = sorted(bookmap.values(), key=lambda e: e["score"], reverse=True)
            final = [e["book"] for e in ranked]

            # ---------- quality pass + top-up
            quality = [b for b in final if (getattr(b, 'rating', None) is None) or (b.rating >= profile.preferred_rating_threshold)]
            needed = total_limit - len(quality)
            if needed > 0:
                extra = self.get_quality_trending_books(needed, [x.id for x in quality], profile)
                quality.extend(extra)
            final = quality[:total_limit]

            # ---------- contributions computed on the CHOSEN set
            contributions = {"content":0,"author":0,"collab":0,"trending":0,"diversity":0}
            chosen_ids = {_canon_id(b.id) for b in final}
            for bid in chosen_ids:
                srcs = bookmap.get(bid, {}).get("srcs", set())
                for s in srcs:
                    contributions[s] = contributions.get(s, 0) + 1

            # ---------- label & confidence
            i_count = count_user_interactions(getattr(profile, 'clerk_user_id', '') or "")
            u_count = count_user_interests(getattr(profile, 'clerk_user_id', '') or "")
            confidence = self._calculate_enhanced_confidence(profile, contributions, total_limit)
            algo_label = "Database Hybrid (Collaborative + Content + Trending)" if (i_count > 0 or u_count > 0) else "Trending (New User)"

            # ---------- ensure cover URLs are usable (http -> https)
            import re
            for b in final:
                url = getattr(b, "cover_image_url", None)
                if url:
                    b.cover_image_url = re.sub(r"(?i)^http://", "https://", url.strip())

            return RecommendationResult(
                books=final,
                algorithm_used=algo_label,
                confidence_score=confidence,
                reasons=list(set(all_reasons)),
                generated_at=datetime.now(),
                contributions=contributions,
                interaction_count=i_count,
            )

        except Exception as e:
            logger.error(f"Error in enhanced hybrid recommendations: {e}")
            trending = self.get_quality_trending_books(total_limit, [], profile)
            return RecommendationResult(
                books=trending,
                algorithm_used="Fallback (Quality Trending)",
                confidence_score=0.6,
                reasons=["High-quality trending books as fallback"],
                generated_at=datetime.now(),
                contributions={"trending": len(trending)},
                interaction_count=count_user_interactions(getattr(profile, 'clerk_user_id', '') or ""),
            )
    
    def content_based_recommendations_enhanced(self, profile: UserProfile, limit: int = 15) -> Tuple[List[Book], List[str]]:
        """Enhanced content-based recommendations using actual book data"""
        recommendations, reasons = [], []
        exclude_ids = (profile.reading_history or []) + (profile.wishlist or [])

        try:
            # If FE hasn't filled genres yet, pull from DB here (no new helper)
            if not getattr(profile, "selected_genres", None) and getattr(profile, "clerk_user_id", None):
                conn = get_db_connection()
                if conn:
                    from psycopg2.extras import RealDictCursor
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        # prefer numeric mapping if present in table; otherwise fall back to clerk id
                        cur.execute("""
                            SELECT COALESCE(CAST(user_id AS TEXT), clerk_user_id) AS key_col FROM user_interests
                            WHERE clerk_user_id = %s OR CAST(user_id AS TEXT) = %s
                            LIMIT 1
                        """, (profile.clerk_user_id, profile.clerk_user_id))
                        # then fetch genres for that identity
                        cur.execute("""
                            SELECT LOWER(genre) AS genre, COUNT(*) AS cnt
                            FROM user_interests
                            WHERE clerk_user_id = %s OR CAST(user_id AS TEXT) = %s
                            GROUP BY 1 ORDER BY cnt DESC
                        """, (profile.clerk_user_id, profile.clerk_user_id))
                        rows = cur.fetchall()
                        if rows:
                            profile.selected_genres = [r["genre"] for r in rows]
                            profile.interaction_weights = {r["genre"]: float(r["cnt"]) for r in rows}
                    conn.close()

            # Weight genres
            genre_scores = {}
            for genre in (profile.selected_genres or []):
                genre_scores[genre.lower()] = 1.0
            for genre, weight in (profile.interaction_weights or {}).items():
                genre_scores[genre.lower()] = genre_scores.get(genre.lower(), 0) + float(weight) * 0.5

            if not genre_scores:
                genre_scores = {'science':0.7,'technology':0.7,'business':0.6,'art':0.5}

            sorted_genres = sorted(genre_scores.items(), key=lambda x: x[1], reverse=True)
            books_per_genre = max(1, limit // max(1,len(sorted_genres)))

            for genre, base in sorted_genres[:5]:
                # inline synonym expansion for your catalog labels
                extra_terms = []
                if genre in ("selfhelp","self-help","self help"):
                    extra_terms = ["conduct of life","personal development","personal growth","self improvement","self-management","motivation"]
                # main query
                genre_books = self.get_books_by_advanced_genre_search(genre, books_per_genre * 2, exclude_ids)
                # try a couple of alternates if needed
                if len(genre_books) < books_per_genre and extra_terms:
                    for t in extra_terms:
                        if len(genre_books) >= books_per_genre*2: break
                        genre_books += self.get_books_by_advanced_genre_search(t, books_per_genre, exclude_ids)

                for book in genre_books:
                    if book.id not in exclude_ids:
                        book.similarity_score = self._calculate_enhanced_content_score(book, profile, base)
                        recommendations.append(book)
                        exclude_ids.append(book.id)

                if genre_books:
                    reasons.append(f"Based on your interest in {genre} books")
                if len(recommendations) >= limit:
                    break

            recommendations.sort(key=lambda x: getattr(x, 'similarity_score', 0), reverse=True)
            recommendations = recommendations[:limit]
            if recommendations:
                reasons.append("Personalized content matching your reading preferences")

        except Exception as e:
            logger.error(f"Error in enhanced content-based recommendations: {e}")

        return recommendations, reasons
    
    def _calculate_enhanced_content_score(self, book: Book, profile: UserProfile, genre_weight: float = 1.0) -> float:
        """Enhanced content scoring with multiple factors"""
        score = 0.0
        
        try:
            # Genre matching (40% of score)
            if book.genre and profile.selected_genres:
                for user_genre in profile.selected_genres:
                    if user_genre.lower() in book.genre.lower() or book.genre.lower() in user_genre.lower():
                        score += 0.4 * genre_weight
                        break
            
            # Interaction weight boost (30% of score)
            if book.genre and book.genre.lower() in profile.interaction_weights:
                normalized_weight = min(profile.interaction_weights[book.genre.lower()] / 10.0, 1.0)
                score += 0.3 * normalized_weight
            
            # Rating boost (20% of score) 
            if book.rating and book.rating >= profile.preferred_rating_threshold:
                rating_score = (book.rating - profile.preferred_rating_threshold) / (5.0 - profile.preferred_rating_threshold)
                score += 0.2 * rating_score
            
            # Author preference boost (10% of score)
            if book.author and book.author in profile.favorite_authors:
                score += 0.1
            
            # Penalty for already interacted books
            if book.id in profile.reading_history:
                score *= 0.5
            if book.id in profile.wishlist:
                score *= 0.3
                
        except Exception as e:
            logger.error(f"Error calculating enhanced content score: {e}")
        
        return min(score, 1.0)
    
    def get_quality_trending_books(self, limit: int, exclude_ids: List[int], profile: UserProfile) -> List[Book]:
        """Get high-quality trending books with preference filtering"""
        try:
            # Use existing trending function but with better filtering
            result = get_trending_books_db('5years', 1, limit * 3)
            
            if result and result.get('books'):
                trending_books = []
                for book_data in result['books']:
                    if book_data['id'] not in exclude_ids:
                        book = Book(
                            id=book_data['id'],
                            title=book_data['title'],
                            author=book_data['author'],
                            genre=book_data.get('genre'),
                            description=book_data.get('description'),
                            cover_image_url=book_data.get('cover_image_url'),
                            rating=book_data.get('rating'),
                            publication_date=book_data.get('publication_date'),
                            pages=book_data.get('pages'),
                            language=book_data.get('language'),
                            isbn=book_data.get('isbn')
                        )
                        
                        # Filter by quality and preference
                        if (not book.rating or book.rating >= profile.preferred_rating_threshold):
                            # Boost score if matches user preferences
                            score = 0.5  # Base trending score
                            if book.genre and any(genre.lower() in book.genre.lower() for genre in profile.selected_genres):
                                score += 0.3
                            if book.author in profile.favorite_authors:
                                score += 0.2
                                
                            book.similarity_score = score
                            trending_books.append(book)
                            
                        if len(trending_books) >= limit:
                            break
                
                return trending_books[:limit]
        
        except Exception as e:
            logger.error(f"Error getting quality trending books: {e}")
        
        return []
    
    def get_diversity_books(self, profile: UserProfile, limit: int, exclude_ids: List[int]) -> List[Book]:
        """Get diverse books from unexplored genres"""
        try:
        
            # Ensure exclude_ids is a list
            if exclude_ids is None:
                exclude_ids = []

            # Get available genres from a sample of books
            sample_books = self.get_all_books_with_metadata(200)
            available_genres = list(set([book.genre.lower() for book in sample_books if book.genre]))
            
            # Find unexplored genres
            user_genres_lower = {genre.lower() for genre in profile.selected_genres}
            interaction_genres = set(profile.interaction_weights.keys())
            explored_genres = user_genres_lower.union(interaction_genres)
            
            unexplored_genres = []
            for genre in available_genres:
                if genre not in explored_genres and not any(explored in genre or genre in explored for explored in explored_genres):
                    unexplored_genres.append(genre)
            
            if not unexplored_genres:
                # If no truly unexplored genres, try expanding existing preferences
                unexplored_genres = random.sample(available_genres, min(3, len(available_genres)))
            
            # Select random genres for diversity
            selected_genres = random.sample(unexplored_genres, min(3, len(unexplored_genres)))
            
            diversity_books = []
            # Prevent division by zero when selected_genres is empty
            books_per_genre = max(1, limit // max(1, len(selected_genres)))
            
            for genre in selected_genres:
                genre_books = self.get_books_by_advanced_genre_search(genre, books_per_genre, exclude_ids)
                for book in genre_books:
                    if book.id not in exclude_ids:
                        book.similarity_score = 0.4  # Lower score for diversity
                        diversity_books.append(book)
                        exclude_ids.append(book.id)
                        
                if len(diversity_books) >= limit:
                    break
            
            return diversity_books[:limit]
            
        except Exception as e:
            logger.error(f"Error getting diversity books: {e}")
            return []
    
    def _calculate_enhanced_confidence(self, profile: UserProfile, contributions: Dict[str, int], target_limit: int) -> float:
        """Enhanced confidence calculation"""
        try:
            base_score = 0.6  # Higher base for enhanced system
            
            # User profile richness (30% of confidence)
            profile_richness = 0.0
            if profile.selected_genres:
                profile_richness += 0.3 * min(len(profile.selected_genres) / 5, 1.0)
            if profile.reading_history:
                profile_richness += 0.3 * min(len(profile.reading_history) / 20, 1.0)
            if profile.favorite_authors:
                profile_richness += 0.2 * min(len(profile.favorite_authors) / 5, 1.0)
            if profile.interaction_weights:
                profile_richness += 0.2 * min(len(profile.interaction_weights) / 5, 1.0)
            
            base_score += profile_richness * 0.3
            
            # Algorithm diversity (20% of confidence)
            active_algorithms = sum(1 for count in contributions.values() if count > 0)
            algorithm_diversity = active_algorithms / len(self.weights)
            base_score += algorithm_diversity * 0.2
            
            # Recommendation coverage (30% of confidence)
            total_recommendations = sum(contributions.values())
            if total_recommendations > 0:
                coverage = min(total_recommendations / target_limit, 1.0)
                base_score += coverage * 0.3
            
            # Quality bonus (20% of confidence)
            if contributions.get('content_based', 0) > 0:
                base_score += 0.1
            if contributions.get('collaborative', 0) > 0:
                base_score += 0.1
            
            return min(base_score, 1.0)
            
        except Exception as e:
            logger.error(f"Error calculating enhanced confidence: {e}")
            return 0.7

    def _hydrate_profile_from_db(self, profile: UserProfile):
        """Fill missing selected_genres and interaction_weights from Postgres user_interests if clerk_user_id is present."""
        try:
            if profile and getattr(profile, 'clerk_user_id', None):
                if not profile.selected_genres or not profile.interaction_weights:
                    sel, w = load_interests_for_profile(profile.clerk_user_id)
                    if not profile.selected_genres:
                        profile.selected_genres = sel
                    if not profile.interaction_weights:
                        profile.interaction_weights = w
        except Exception as e:
            logger.debug(f"_hydrate_profile_from_db failed: {e}")
    
    def get_recommendations_for_user_enhanced(self, user_id: str, email: str = None, 
                                           selected_genres: List[str] = None, 
                                           limit: int = 20, force_refresh: bool = False) -> RecommendationResult:
        """Enhanced recommendation generation with better caching and fallbacks"""
        try:
            # Check cache first unless force refresh
            if not force_refresh:
                cached_result = self._get_cached_recommendations(user_id)
                if cached_result:
                    logger.info(f"Returning cached enhanced recommendations for {user_id}")
                    return cached_result
            
            # Build enhanced user profile
            profile = self.get_user_profile_enhanced(user_id, email, selected_genres)
            
            # Generate enhanced recommendations
            result = self.hybrid_recommendations_enhanced(profile, limit)
            
            # Cache the result
            self._cache_recommendations(user_id, result)
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting enhanced recommendations for {user_id}: {e}")
            
            # Multi-level fallback system
            try:
                # Fallback 1: Try basic profile with trending
                basic_profile = UserProfile(user_id=user_id, email=email or "", selected_genres=selected_genres or [])
                trending_books = self.get_quality_trending_books(limit, [], basic_profile)
                return RecommendationResult(
                    books=trending_books,
                    algorithm_used="Fallback (Quality Trending)",
                    confidence_score=0.5,
                    reasons=["Quality trending books as fallback"],
                    generated_at=datetime.now()
                )
            except:
                # Fallback 2: Pure trending
                try:
                    result = get_trending_books_db('weekly', 1, limit)
                    books = []
                    if result and result.get('books'):
                        for book_data in result['books'][:limit]:
                            book = Book(
                                id=book_data['id'],
                                title=book_data['title'],
                                author=book_data['author'],
                                genre=book_data.get('genre'),
                                rating=book_data.get('rating')
                            )
                            books.append(book)
                    
                    return RecommendationResult(
                        books=books,
                        algorithm_used="Fallback (Pure Trending)",
                        confidence_score=0.3,
                        reasons=["Basic trending books as emergency fallback"],
                        generated_at=datetime.now()
                    )
                except:
                    pass
            
            # Fallback 3: Empty result
            return RecommendationResult(
                books=[],
                algorithm_used="Fallback (Empty)",
                confidence_score=0.0,
                reasons=["Unable to generate recommendations"],
                generated_at=datetime.now()
            )
    
    def hybrid_recommendations(self, profile: UserProfile, total_limit: int = 20) -> RecommendationResult:
        """Generate hybrid recommendations combining multiple algorithms"""
        try:
            # hydrate profile from Postgres if clerk_user_id present
            try:
                self._hydrate_profile_from_db(profile)
            except Exception:
                pass
            all_recommendations = []
            all_reasons = []
            algorithm_contributions = {}
            
            # 1. Content-based recommendations (40%)
            content_limit = int(total_limit * self.weights['content_based'])
            content_books, content_reasons = self.content_based_recommendations(profile, content_limit)
            all_recommendations.extend(content_books)
            all_reasons.extend(content_reasons)
            algorithm_contributions['content_based'] = len(content_books)
            
            # 2. Collaborative filtering (Postgres)
            collab_limit = int(total_limit * self.weights['collaborative'])
            # Accept both clerk_user_id and numeric user_id from profile
            clerk_id = getattr(profile, 'clerk_user_id', None)
            numeric_id = None
            try:
                # profile.user_id may be string; try to convert
                numeric_id = int(profile.user_id) if isinstance(profile.user_id, (int, str)) and str(profile.user_id).isdigit() else None
            except Exception:
                numeric_id = None

            collab_rows, collab_reasons = collaborative_filtering_recommendations_pg(
                clerk_user_id=clerk_id,
                user_id=numeric_id,
                limit=collab_limit
            )
            # Convert DB rows to Book objects and dedupe against existing recommendations
            collab_books = []
            existing_ids = {b.id for b in all_recommendations}
            for r in collab_rows:
                b = self._make_book_from_row(r)
                if b and b.id not in existing_ids:
                    collab_books.append(b)
                    existing_ids.add(b.id)

            all_recommendations.extend(collab_books)
            all_reasons.extend(collab_reasons)
            algorithm_contributions['collaborative'] = len(collab_books)
            
            # 3. Trending books (20%)
            trending_limit = int(total_limit * self.weights['trending'])
            exclude_ids = [b.id for b in all_recommendations] + profile.reading_history + profile.wishlist
            trending_books = self._fetch_trending_books(trending_limit * 2, exclude_ids)
            trending_books = trending_books[:trending_limit]
            all_recommendations.extend(trending_books)
            if trending_books:
                all_reasons.append("Currently trending and popular books")
            algorithm_contributions['trending'] = len(trending_books)
            
            # 4. Diversity injection (10%)
            diversity_limit = int(total_limit * self.weights['diversity'])
            diversity_books, diversity_reasons = self.diversity_recommendations(profile, diversity_limit)
            # Remove duplicates
            diversity_books = [book for book in diversity_books 
                             if book.id not in {b.id for b in all_recommendations}]
            all_recommendations.extend(diversity_books)
            all_reasons.extend(diversity_reasons)
            algorithm_contributions['diversity'] = len(diversity_books)
            
            # Calculate overall confidence score
            confidence = self._calculate_confidence_score(profile, algorithm_contributions, total_limit)
            
            # Remove duplicates and limit results
            seen_ids = set()
            final_recommendations = []
            for book in all_recommendations:
                if book.id not in seen_ids:
                    seen_ids.add(book.id)
                    final_recommendations.append(book)
                if len(final_recommendations) >= total_limit:
                    break
            
            # If we don't have enough recommendations, fill with trending books
            if len(final_recommendations) < total_limit * 0.5:
                logger.info(f"Low recommendation count ({len(final_recommendations)}), adding trending books")
                exclude_ids = [b.id for b in final_recommendations]
                additional_trending = self._fetch_trending_books(total_limit - len(final_recommendations), exclude_ids)
                final_recommendations.extend(additional_trending)
            
            # Determine primary algorithm used
            if algorithm_contributions:
                primary_algorithm = max(algorithm_contributions.items(), key=lambda x: x[1])[0]
            else:
                primary_algorithm = "trending"
            
            result = RecommendationResult(
                books=final_recommendations,
                algorithm_used=f"Hybrid ({primary_algorithm} primary)",
                confidence_score=confidence,
                reasons=list(set(all_reasons)),  # Remove duplicate reasons
                generated_at=datetime.now()
            )
            
            logger.info(f"Generated {len(final_recommendations)} hybrid recommendations for {profile.user_id}")
            logger.info(f"Algorithm contributions: {algorithm_contributions}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error in hybrid recommendations: {e}")
            
            # Fallback to trending books
            trending_books = self._fetch_trending_books(total_limit)
            return RecommendationResult(
                books=trending_books,
                algorithm_used="Fallback (Trending)",
                confidence_score=0.5,
                reasons=["Fallback recommendations based on popular books"],
                generated_at=datetime.now()
            )
    
    def _calculate_confidence_score(self, profile: UserProfile, contributions: Dict[str, int], target_limit: int) -> float:
        """Calculate confidence score for recommendations"""
        try:
            # Base score on data availability
            base_score = 0.5
            
            # Boost for user data richness
            if profile.selected_genres:
                base_score += 0.1 * min(len(profile.selected_genres) / 5, 1.0)
            
            if profile.reading_history:
                base_score += 0.1 * min(len(profile.reading_history) / 10, 1.0)
            
            if profile.interaction_weights:
                base_score += 0.1 * min(len(profile.interaction_weights) / 3, 1.0)
            
            # Boost for algorithm diversity
            active_algorithms = sum(1 for count in contributions.values() if count > 0)
            base_score += 0.1 * (active_algorithms / 4)  # Max 4 algorithms
            
            # Penalize if we couldn't generate enough recommendations
            total_recommendations = sum(contributions.values())
            if total_recommendations > 0:
                recommendation_ratio = total_recommendations / target_limit
                base_score *= min(recommendation_ratio, 1.0)
            else:
                base_score *= 0.5  # Significant penalty for no recommendations
            
            return min(base_score, 1.0)
            
        except Exception as e:
            logger.error(f"Error calculating confidence: {e}")
            return 0.5
    
    def get_recommendations_for_user(self, user_id: str, email: str = None, 
                                   selected_genres: List[str] = None, 
                                   limit: int = 20, force_refresh: bool = False) -> RecommendationResult:
        """Get recommendations for a user with caching"""
        try:
            # Check cache first unless force refresh
            if not force_refresh:
                cached_result = self._get_cached_recommendations(user_id)
                if cached_result:
                    logger.info(f"Returning cached recommendations for {user_id}")
                    return cached_result
            
            # Build user profile
            profile = self.get_user_profile(user_id, email, selected_genres)
            
            # Generate recommendations
            result = self.hybrid_recommendations(profile, limit)
            
            # Cache the result
            self._cache_recommendations(user_id, result)
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting recommendations for {user_id}: {e}")
            
            # Return trending books as fallback
            trending_books = self._fetch_trending_books(limit)
            return RecommendationResult(
                books=trending_books,
                algorithm_used="Fallback (Trending)",
                confidence_score=0.5,
                reasons=["Fallback recommendations due to system error"],
                generated_at=datetime.now()
            )
    
    def _get_cached_recommendations(self, user_id: str) -> Optional[RecommendationResult]:
        """Get cached recommendations if still valid"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT recommendations, algorithm_used, generated_at
                FROM user_recommendations
                WHERE user_id = ? AND expires_at > ?
            ''', (user_id, datetime.now()))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                recommendations_data, algorithm_used, generated_at = result
                import json
                data = json.loads(recommendations_data)
                
                books = [Book(**book_data) for book_data in data['books']]
                
                return RecommendationResult(
                    books=books,
                    algorithm_used=algorithm_used,
                    confidence_score=data['confidence_score'],
                    reasons=data['reasons'],
                    generated_at=datetime.fromisoformat(generated_at)
                )
                
        except Exception as e:
            logger.error(f"Error getting cached recommendations: {e}")
            
        return None
    
    def _cache_recommendations(self, user_id: str, result: RecommendationResult):
        """Cache recommendations with proper JSON serialization"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Prepare data for caching with safe serialization
            cache_data = {
                'books': [serialize_for_cache(book) for book in result.books],
                'confidence_score': float(result.confidence_score),
                'reasons': result.reasons
            }
            
            # Cache expires in 24 hours
            expires_at = datetime.now() + timedelta(hours=24)
            
            cursor.execute('''
                INSERT OR REPLACE INTO user_recommendations 
                (user_id, recommendations, algorithm_used, expires_at)
                VALUES (?, ?, ?, ?)
            ''', (user_id, json.dumps(cache_data, default=str), result.algorithm_used, expires_at))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Cached recommendations for {user_id}")
            
        except Exception as e:
            logger.error(f"Error caching recommendations: {e}")
    
    def get_user_interests_from_db(self, user_id: str) -> List[str]:
        """Get user's selected genres from the main database"""
        try:
            conn = get_db_connection()
            if not conn:
                return []
                
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT genre FROM user_interests 
                    WHERE user_id = %s
                """, (user_id,))
                
                results = cursor.fetchall()
                conn.close()
                
                return [row['genre'] for row in results]
                
        except Exception as e:
            logger.error(f"Error getting user interests from database: {e}")
            return []
    
    def get_personalized_trending(self, user_id: str, limit: int = 10) -> RecommendationResult:
        """Get trending books filtered by user preferences"""
        try:
            profile = self.get_user_profile(user_id)
            exclude_ids = profile.reading_history + profile.wishlist
            
            # Get trending books
            trending_books = self._fetch_trending_books(limit * 3, exclude_ids)
            
            # Filter and score by user preferences
            scored_books = []
            for book in trending_books:
                score = self._calculate_content_score(book, profile)
                if score > 0.2:  # Lower threshold for trending
                    book.trend_score = score
                    scored_books.append(book)
            
            # Sort by score and take top results
            scored_books.sort(key=lambda x: getattr(x, 'trend_score', 0), reverse=True)
            final_books = scored_books[:limit]
            
            reasons = ["Trending books that match your interests"]
            if profile.selected_genres:
                reasons.append(f"Filtered by your preferred genres: {', '.join(profile.selected_genres[:3])}")
            
            return RecommendationResult(
                books=final_books,
                algorithm_used="Personalized Trending",
                confidence_score=0.8,
                reasons=reasons,
                generated_at=datetime.now()
            )
            
        except Exception as e:
            logger.error(f"Error getting personalized trending: {e}")
            return self.get_recommendations_for_user(user_id, limit=limit)
    
    def get_recommendation_stats(self, user_id: str) -> Dict:
        """Get statistics about user's recommendation history"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get interaction stats
            cursor.execute('''
                SELECT interaction_type, COUNT(*) as count
                FROM user_interactions
                WHERE user_id = ?
                GROUP BY interaction_type
            ''', (user_id,))
            
            interaction_stats = dict(cursor.fetchall())
            
            # Get recommendation cache info
            cursor.execute('''
                SELECT generated_at, algorithm_used
                FROM user_recommendations
                WHERE user_id = ?
                ORDER BY generated_at DESC
                LIMIT 1
            ''', (user_id,))
            
            cache_info = cursor.fetchone()
            conn.close()
            
            stats = {
                'total_interactions': sum(interaction_stats.values()),
                'views': interaction_stats.get('view', 0),
                'wishlist_items': interaction_stats.get('wishlist', 0),
                'searches': interaction_stats.get('search', 0),
                'last_recommendation': cache_info[0] if cache_info else None,
                'last_algorithm': cache_info[1] if cache_info else None
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting recommendation stats: {e}")
            return {}
    
    def cleanup_old_data(self, days_to_keep: int = 90):
        """Clean up old interaction data and cache entries"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            # Clean old interactions
            cursor.execute('''
                DELETE FROM user_interactions 
                WHERE timestamp < ?
            ''', (cutoff_date,))
            
            interactions_deleted = cursor.rowcount
            
            # Clean expired cache entries
            cursor.execute('''
                DELETE FROM user_recommendations 
                WHERE expires_at < ?
            ''', (datetime.now(),))
            
            cache_deleted = cursor.rowcount
            
            # Clean old book cache (keep for 7 days)
            cursor.execute('''
                DELETE FROM book_cache 
                WHERE last_updated < ?
            ''', (datetime.now() - timedelta(days=7),))
            
            books_deleted = cursor.rowcount
            
            conn.commit()
            conn.close()
            
            logger.info(f"Cleanup complete: {interactions_deleted} interactions, {cache_deleted} cache entries, {books_deleted} book cache entries deleted")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

# Genre mapping for normalizing user interests to book genres
GENRE_MAP = {
    "art": "Art",
    "business": "Business & Economics", 
    "science": "Science",
    "technology": "Science & Technology",  # Updated mapping
    "selfhelp": "Self Help",
    "self-help": "Self Help",
    "fiction": "Fiction",
    "history": "History",
    "sports": "Sports & Recreation",
    "recreation": "Sports & Recreation", 
    "language": "Foreign Language Study",
    "foreign language": "Foreign Language Study",
    "german": "German language",
    "philosophy": "Philosophy",
    "religion": "Religion",
    "psychology": "Psychology",
    "biography": "Biography & Autobiography",
    "autobiography": "Biography & Autobiography",
    "cooking": "Cooking",
    "health": "Health & Fitness",
    "fitness": "Health & Fitness",
    "travel": "Travel",
    "romance": "Romance",
    "mystery": "Mystery",
    "thriller": "Thriller",
    "fantasy": "Fantasy",
    "science fiction": "Science Fiction",
    "sci-fi": "Science Fiction",
    "horror": "Horror",
    "young adult": "Young Adult Fiction",
    "children": "Juvenile Fiction",
    "education": "Education",
    "politics": "Political Science",
    "law": "Law",
    "medicine": "Medical",
    "mathematics": "Mathematics",
    "computer": "Computers",
    "nature": "Nature",
    "music": "Music",
    "drama": "Drama",
    "poetry": "Poetry",
    "comics": "Comics & Graphic Novels",
    "graphic novels": "Comics & Graphic Novels"
}

def normalize_genres(user_genres):
    """
    Normalize user interest genres to match book database genre format.
    
    Args:
        user_genres: List of genres from user_interests table
        
    Returns:
        List of normalized genre names that match the books table
    """
    if not user_genres:
        return []
    
    normalized = []
    for genre in user_genres:
        if not genre:
            continue
        # Try exact match first (case-insensitive)
        original_genre = genre.lower().strip()
        normalized_genre = GENRE_MAP.get(original_genre, genre.strip())
        normalized.append(normalized_genre)
        
        # Debug logging
        if original_genre != normalized_genre.lower():
            logger.info(f"Genre mapping: '{original_genre}' -> '{normalized_genre}'")
    
    logger.info(f"Normalized genres: {user_genres} -> {normalized}")
    return normalized


def load_interests_for_profile(clerk_user_id: str):
    """Return (selected_genres:list[str], weights:dict[genre->score]) from user_interests."""
    conn = get_db_connection()
    if not conn:
        return [], {}
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT LOWER(genre), COUNT(*) AS cnt
                FROM public.user_interests
                WHERE clerk_user_id = %s
                GROUP BY 1
                ORDER BY cnt DESC
            """, (clerk_user_id,))
            rows = cur.fetchall()
            selected = [r[0] for r in rows]
            weights  = {r[0]: float(r[1]) for r in rows}
            return selected, weights
    finally:
        try:
            conn.close()
        except Exception:
            pass

# Simple function for basic Flask integration
def recommend_books_for_user(user_id, limit=20):
    """
    Simple function to get book recommendations for a user using psycopg2.
    Returns a list of book dictionaries compatible with the simple routes.
    """
    from libby_backend.database import get_db_connection
    
    conn = get_db_connection()
    cur = conn.cursor()

    # Get user genres
    cur.execute("SELECT genre FROM user_interests WHERE user_id = %s", (user_id,))
    genres = [row["genre"].lower() for row in cur.fetchall()]
    print("🔍 User genres:", genres)

    if not genres:
        print("🔍 No user genres found, returning empty list")
        cur.close()
        conn.close()
        return []

    # Build OR conditions
    conditions = " OR ".join(["LOWER(genre) LIKE %s" for _ in genres])
    sql = f"""
        SELECT book_id, isbn, title, author, genre, description, rating, cover_image_url
        FROM books
        WHERE {conditions}
        ORDER BY rating DESC NULLS LAST
        LIMIT %s
    """
    params = [f"%{g}%" for g in genres] + [limit]

    # Debug logs
    print("🔍 SQL:", sql)
    print("🔍 Params:", params)

    cur.execute(sql, params)
    rows = cur.fetchall()
    print("row fetch:", rows)
    print("first few rows:", rows[:3] )

    books = []
    for row in rows:
        # Ensure we always have a cover image URL
        cover_url = row[7] or f"https://placehold.co/320x480/1e1f22/ffffff?text={row[2]}"
        
        books.append({
            "id": row[0],
            "isbn": row[1],
            "title": row[2],
            "author": row[3],
            "genre": row[4],
            "description": row[5],
            "rating": float(row[6]) if row[6] is not None else None,
            "coverurl": cover_url,
            "cover_image_url": cover_url,  # Add this for consistency
        })

    cur.close()
    conn.close()
    return books

# Debug function removed for production use
# def debug_genre_matching(user_genres, limit=5):
#     """Debug function removed for production"""
#     return {"message": "Debug functions disabled in production"}

# Debug function removed for production use  
# def get_actual_genres_in_database():
#     """Debug function removed for production"""  
#     return []

def improved_genre_matching(user_genre):
    """Improved genre matching with better mappings"""
    
    # Enhanced genre mappings
    genre_mappings = {
        'art': ['art', 'design', 'photography', 'painting', 'sculpture', 'visual arts'],
        'business': ['business', 'economics', 'management', 'entrepreneurship', 'finance', 'marketing'],
        'fiction': ['fiction', 'novel', 'literary fiction', 'contemporary fiction'],
        'science': ['science', 'physics', 'chemistry', 'biology', 'scientific', 'research'],
        'technology': ['technology', 'computer', 'programming', 'tech', 'digital', 'software', 'engineering']
    }
    
    # Get all possible search terms for this genre
    search_terms = [user_genre.lower()]
    if user_genre.lower() in genre_mappings:
        search_terms.extend(genre_mappings[user_genre.lower()])
    
    return search_terms

def get_books_for_user_genre(user_genre, limit=20, exclude_ids=None):
    """Get books for a specific user genre with improved matching"""
    from libby_backend.database import get_db_connection
    
    if exclude_ids is None:
        exclude_ids = []
    
    conn = get_db_connection()
    if not conn:
        return []
    
    books = []
    search_terms = improved_genre_matching(user_genre)
    
    try:
        with conn.cursor() as cursor:
            for term in search_terms:
                if len(books) >= limit:
                    break
                    
                exclude_clause = ""
                params = [f'%{term}%']
                
                if exclude_ids:
                    placeholders = ','.join(['%s'] * len(exclude_ids))
                    exclude_clause = f"AND book_id NOT IN ({placeholders})"
                    params.extend(exclude_ids)
                
                params.append(limit - len(books))
                
                query = f"""
                    SELECT book_id, title, author, genre, description, cover_image_url, 
                           rating, publication_date, pages, language, isbn
                    FROM books 
                    WHERE LOWER(genre) ILIKE %s {exclude_clause}
                    AND title IS NOT NULL 
                    AND author IS NOT NULL
                    ORDER BY rating DESC NULLS LAST, book_id
                    LIMIT %s
                """
                
                cursor.execute(query, params)
                rows = cursor.fetchall()
                
                for row in rows:
                    if row[0] not in exclude_ids and len(books) < limit:
                        book = Book(
                            id=row[0],
                            title=row[1],
                            author=row[2],
                            genre=row[3],
                            description=row[4],
                            cover_image_url=row[5],
                            rating=float(row[6]) if row[6] is not None else None,
                            publication_date=str(row[7]) if row[7] else None,
                            pages=row[8],
                            language=row[9],
                            isbn=row[10]
                        )
                        books.append(book)
                        exclude_ids.append(row[0])
        
        conn.close()
        print(f"Found {len(books)} books for genre '{user_genre}' using terms: {search_terms}")
        return books
        
    except Exception as e:
        print(f"Error getting books for genre {user_genre}: {e}")
        conn.close()
        return []

def get_personalized_recommendations_fixed(user_id, limit=20):
    """Fixed personalized recommendations that actually use user preferences"""
    from libby_backend.database import get_db_connection
    from datetime import datetime
    
    # 1. Get user's actual genres from database
    conn = get_db_connection()
    if not conn:
        return None
    
    user_genres = []
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT genre FROM user_interests WHERE user_id = %s", (user_id,))
            user_genres = [row[0] for row in cursor.fetchall()]
        conn.close()
    except Exception as e:
        print(f"Error getting user genres: {e}")
        conn.close()
        return None
    
    if not user_genres:
        print(f"No genres found for user {user_id}")
        return None
    
    print(f"User {user_id} has genres: {user_genres}")
    
    # 2. Get books for each genre
    all_books = []
    books_per_genre = max(1, limit // len(user_genres))
    exclude_ids = []
    
    for genre in user_genres:
        genre_books = get_books_for_user_genre(genre, books_per_genre + 2, exclude_ids)
        for book in genre_books:
            if len(all_books) < limit:
                book.similarity_score = 0.8  # High score for user preferences
                all_books.append(book)
                exclude_ids.append(book.id)
    
    print(f"Found {len(all_books)} personalized books for user {user_id}")
    
    # 3. If we don't have enough books, fill with high-rated books from user genres
    if len(all_books) < limit * 0.5:  # Less than half the requested amount
        print("Not enough genre-specific books, adding high-rated books...")
        
        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                # Get high-rated books from any genre
                exclude_clause = ""
                params = []
                
                if exclude_ids:
                    placeholders = ','.join(['%s'] * len(exclude_ids))
                    exclude_clause = f"WHERE book_id NOT IN ({placeholders})"
                    params.extend(exclude_ids)
                
                params.append(limit - len(all_books))
                
                query = f"""
                    SELECT book_id, title, author, genre, description, cover_image_url, 
                           rating, publication_date, pages, language, isbn
                    FROM books 
                    {exclude_clause}
                    {'AND' if exclude_clause else 'WHERE'} rating >= 4.0
                    AND title IS NOT NULL 
                    AND author IS NOT NULL
                    ORDER BY rating DESC, book_id
                    LIMIT %s
                """
                
                cursor.execute(query, params)
                rows = cursor.fetchall()
                
                for row in rows:
                    book = Book(
                        id=row[0],
                        title=row[1],
                        author=row[2],
                        genre=row[3],
                        description=row[4],
                        cover_image_url=row[5],
                        rating=float(row[6]) if row[6] is not None else None,
                        publication_date=str(row[7]) if row[7] else None,
                        pages=row[8],
                        language=row[9],
                        isbn=row[10]
                    )
                    book.similarity_score = 0.6  # Lower score for general recommendations
                    all_books.append(book)
            
            conn.close()
        except Exception as e:
            print(f"Error getting high-rated books: {e}")
            if conn:
                conn.close()
    
    # 4. Create result
    if all_books:
        reasons = [f"Based on your interests: {', '.join(user_genres)}"]
        if len(all_books) < limit * 0.7:
            reasons.append("Supplemented with high-rated books due to limited genre matches")
        
        return RecommendationResult(
            books=all_books,
            algorithm_used="Fixed Personalized Recommendations",
            confidence_score=0.85 if len(all_books) >= limit * 0.7 else 0.65,
            reasons=reasons,
            generated_at=datetime.now()
        )
    else:
        return None

# Enhanced Flask API Integration Classes
class EnhancedRecommendationAPI:
    """Enhanced Flask API endpoints for the recommendation system"""
    
    def __init__(self, recommendation_engine: EnhancedBookRecommendationEngine):
        self.engine = recommendation_engine
    
    def get_user_recommendations(self, user_id: str, email: str = None, 
                               genres: List[str] = None, limit: int = 20) -> Dict:
        """Enhanced API endpoint for getting user recommendations"""
        try:
            result = self.engine.get_recommendations_for_user_enhanced(
                user_id=user_id,
                email=email,
                selected_genres=genres,
                limit=limit
            )
            
            return {
                "success": True,
                "recommendations": [book.__dict__ for book in result.books],
                "algorithm_used": result.algorithm_used,
                "confidence_score": result.confidence_score,
                "reasons": result.reasons,
                "generated_at": result.generated_at.isoformat(),
                "total_count": len(result.books)
            }
            
        except Exception as e:
            logger.error(f"Error in enhanced API get_user_recommendations: {e}")
            return {
                "success": False,
                "error": str(e),
                "recommendations": []
            }
    
    def record_interaction(self, user_id: str, book_id: int, interaction_type: str, rating: float = None) -> Dict:
        """Enhanced API endpoint for recording user interactions"""
        try:
            # Map interaction types to weights
            weight_mapping = {
                'view': 1.0,
                'like': 2.0,
                'wishlist_add': 3.0,
                'wishlist_remove': -1.0,
                'rate': 2.5,
                'search': 0.5
            }
            
            weight = weight_mapping.get(interaction_type, 1.0)
            
            self.engine.record_user_interaction(
                user_id=user_id,
                book_id=book_id,
                interaction_type=interaction_type,
                weight=weight,
                rating=rating
            )
            
            return {
                "success": True,
                "message": f"Interaction recorded: {interaction_type} for book {book_id}",
                "user_id": user_id,
                "book_id": book_id,
                "interaction_type": interaction_type,
                "weight": weight,
                "rating": rating
            }
            
        except Exception as e:
            logger.error(f"Error in enhanced API record_interaction: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def get_personalized_trending(self, user_id: str, limit: int = 10) -> Dict:
        """Enhanced API endpoint for personalized trending books"""
        try:
            # Build user profile for filtering
            profile = self.engine.get_user_profile_enhanced(user_id)
            
            # Get quality trending books
            trending_books = self.engine.get_quality_trending_books(limit, [], profile)
            
            return {
                "success": True,
                "trending_books": [book.__dict__ for book in trending_books],
                "personalized": True,
                "user_id": user_id,
                "total_count": len(trending_books)
            }
            
        except Exception as e:
            logger.error(f"Error in enhanced API get_personalized_trending: {e}")
            return {
                "success": False,
                "error": str(e),
                "trending_books": []
            }
    
    def refresh_recommendations(self, user_id: str, limit: int = 20) -> Dict:
        """Enhanced API endpoint for forcing recommendation refresh"""
        try:
            result = self.engine.get_recommendations_for_user_enhanced(
                user_id=user_id,
                limit=limit,
                force_refresh=True
            )
            
            return {
                "success": True,
                "message": "Recommendations refreshed successfully",
                "recommendations": [book.__dict__ for book in result.books],
                "algorithm_used": result.algorithm_used,
                "confidence_score": result.confidence_score,
                "reasons": result.reasons,
                "generated_at": result.generated_at.isoformat(),
                "total_count": len(result.books)
            }
            
        except Exception as e:
            logger.error(f"Error in enhanced API refresh_recommendations: {e}")
            return {
                "success": False,
                "error": str(e),
                "recommendations": []
            }

# Flask API Integration Classes
class RecommendationAPI:
    """Flask API endpoints for the recommendation system"""
    
    def __init__(self, recommendation_engine: EnhancedBookRecommendationEngine):
        self.engine = recommendation_engine
    
    def get_user_recommendations(self, user_id: str, email: str = None, 
                               genres: List[str] = None, limit: int = 20) -> Dict:
        """API endpoint for getting user recommendations"""
        try:
            # Get user's selected genres from database if not provided
            if not genres:
                genres = self.engine.get_user_interests_from_db(user_id)
            
            result = self.engine.get_recommendations_for_user(
                user_id=user_id,
                email=email, 
                selected_genres=genres,
                limit=limit
            )
            
            return {
                'success': True,
                'books': [book.__dict__ for book in result.books],
                'algorithm': result.algorithm_used,
                'confidence': result.confidence_score,
                'reasons': result.reasons,
                'generated_at': result.generated_at.isoformat(),
                'total_count': len(result.books)
            }
            
        except Exception as e:
            logger.error(f"Error in API get_user_recommendations: {e}")
            return {
                'success': False,
                'error': str(e),
                'books': []
            }
    
    def get_personalized_trending(self, user_id: str, limit: int = 10) -> Dict:
        """API endpoint for personalized trending books"""
        try:
            result = self.engine.get_personalized_trending(user_id, limit)
            
            return {
                'success': True,
                'books': [book.__dict__ for book in result.books],
                'algorithm': result.algorithm_used,
                'confidence': result.confidence_score,
                'reasons': result.reasons,
                'generated_at': result.generated_at.isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error in API get_personalized_trending: {e}")
            return {
                'success': False,
                'error': str(e),
                'books': []
            }
    
    def record_interaction(self, user_id: str, book_id: int, interaction_type: str) -> Dict:
        """API endpoint for recording user interactions"""
        try:
            # Define interaction weights
            weight_map = {
                'view': 1.0,
                'wishlist_add': 2.0,
                'wishlist_remove': -1.0,
                'search': 0.5,
                'click': 0.8,
                'like': 1.5,
                'dislike': -0.5
            }
            
            weight = weight_map.get(interaction_type, 1.0)
            
            self.engine.record_user_interaction(user_id, book_id, interaction_type, weight)
            
            return {
                'success': True,
                'message': f'Recorded {interaction_type} for book {book_id}'
            }
            
        except Exception as e:
            logger.error(f"Error recording interaction: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_user_stats(self, user_id: str) -> Dict:
        """API endpoint for user recommendation statistics"""
        try:
            stats = self.engine.get_recommendation_stats(user_id)
            
            return {
                'success': True,
                'stats': stats
            }
            
        except Exception as e:
            logger.error(f"Error getting user stats: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def refresh_recommendations(self, user_id: str, limit: int = 20) -> Dict:
        """API endpoint for forcing recommendation refresh"""
        try:
            result = self.engine.get_recommendations_for_user(
                user_id=user_id,
                limit=limit,
                force_refresh=True
            )
            
            return {
                'success': True,
                'books': [book.__dict__ for book in result.books],
                'algorithm': result.algorithm_used,
                'confidence': result.confidence_score,
                'reasons': result.reasons,
                'generated_at': result.generated_at.isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error refreshing recommendations: {e}")
            return {
                'success': False,
                'error': str(e)
            }