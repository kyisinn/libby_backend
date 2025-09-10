# Book Recommendation System for LIBBY BOT - Database Integrated Version
# Advanced recommendation engine with direct database access

import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict, Counter
import logging
import os
import random
import math

# Import your existing database functions
from libby_backend.database import (
    get_db_connection, search_books_db, get_trending_books_db, 
    get_books_by_major_db, get_book_by_id_db, get_books_by_genre_db
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

@dataclass
class UserProfile:
    """User profile with preferences and interaction history"""
    user_id: str
    email: str
    selected_genres: List[str]
    reading_history: List[int] = None  # Book IDs user has viewed/read
    wishlist: List[int] = None         # Book IDs in user's wishlist
    search_history: List[str] = None   # Search terms used
    interaction_weights: Dict[str, float] = None  # Genre weights based on interactions
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

@dataclass
class RecommendationResult:
    """Container for recommendation results"""
    books: List[Book]
    algorithm_used: str
    confidence_score: float
    reasons: List[str]
    generated_at: datetime

class BookRecommendationEngine:
    """
    Advanced book recommendation engine with direct database integration
    """
    
    def __init__(self):
        self.db_path = os.getenv('RECOMMENDATION_DB_PATH', 'recommendations.db')
        
        # Algorithm weights
        self.weights = {
            'content_based': 0.4,
            'collaborative': 0.3,
            'trending': 0.2,
            'diversity': 0.1
        }
        
        self._init_database()
    
    def _init_database(self):
        """Initialize SQLite database for storing recommendation data"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # User interactions table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_interactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    book_id INTEGER NOT NULL,
                    interaction_type TEXT NOT NULL, -- 'view', 'wishlist', 'search'
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    weight REAL DEFAULT 1.0
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
            
            conn.commit()
            conn.close()
            
            logger.info("Recommendation database initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing recommendation database: {e}")
    
    def record_user_interaction(self, user_id: str, book_id: int, interaction_type: str, weight: float = 1.0):
        """Record user interaction with a book"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO user_interactions (user_id, book_id, interaction_type, weight)
                VALUES (?, ?, ?, ?)
            ''', (user_id, book_id, interaction_type, weight))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Recorded interaction: {user_id} -> {book_id} ({interaction_type})")
            
        except Exception as e:
            logger.error(f"Error recording interaction: {e}")
    
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
            
            profile = UserProfile(
                user_id=user_id,
                email=email or f"user_{user_id}@example.com",
                selected_genres=selected_genres or [],
                reading_history=reading_history,
                wishlist=wishlist,
                interaction_weights=dict(interaction_weights)
            )
            
            return profile
            
        except Exception as e:
            logger.error(f"Error getting user profile: {e}")
            return UserProfile(user_id=user_id, email=email or "", selected_genres=selected_genres or [])
    
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
        """Generate recommendations based on similar users"""
        recommendations = []
        reasons = []
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Find users with similar interactions
            cursor.execute('''
                SELECT ui2.user_id, COUNT(*) as common_books,
                       GROUP_CONCAT(ui2.book_id) as their_books
                FROM user_interactions ui1
                JOIN user_interactions ui2 ON ui1.book_id = ui2.book_id 
                    AND ui1.user_id != ui2.user_id
                WHERE ui1.user_id = ? AND ui1.interaction_type IN ('view', 'wishlist')
                    AND ui2.interaction_type IN ('view', 'wishlist')
                GROUP BY ui2.user_id
                HAVING common_books >= 2
                ORDER BY common_books DESC
                LIMIT 10
            ''', (profile.user_id,))
            
            similar_users = cursor.fetchall()
            
            if similar_users:
                # Get books liked by similar users that current user hasn't seen
                recommended_book_ids = Counter()
                
                for similar_user_id, common_books, their_books in similar_users:
                    their_book_ids = [int(book_id) for book_id in their_books.split(',')]
                    
                    # Weight recommendations by similarity (number of common books)
                    similarity_weight = min(common_books / 5.0, 1.0)
                    
                    for book_id in their_book_ids:
                        if book_id not in profile.reading_history and book_id not in profile.wishlist:
                            recommended_book_ids[book_id] += similarity_weight
                
                # Get top recommended books from main database
                top_book_ids = [book_id for book_id, _ in recommended_book_ids.most_common(limit * 2)]
                
                for book_id in top_book_ids:
                    book = self._get_book_from_main_db(book_id)
                    if book:
                        book.collaborative_score = recommended_book_ids[book_id]
                        recommendations.append(book)
                
                recommendations = recommendations[:limit]
                reasons.append("Users with similar reading preferences also enjoyed these books")
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Error in collaborative filtering: {e}")
        
        return recommendations, reasons
    
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
                    genre_books = self._fetch_books_by_genre(genre, books_per_genre * 2, exclude_ids)
                    
                    # Prefer highly rated books for diversity recommendations
                    genre_books.sort(key=lambda x: x.rating or 0, reverse=True)
                    recommendations.extend(genre_books[:books_per_genre])
                
                if recommendations:
                    reasons.append(f"Discover new genres: {', '.join(selected_genres)}")
            
        except Exception as e:
            logger.error(f"Error in diversity recommendations: {e}")
        
        return recommendations[:limit], reasons
    
    def hybrid_recommendations(self, profile: UserProfile, total_limit: int = 20) -> RecommendationResult:
        """Generate hybrid recommendations combining multiple algorithms"""
        try:
            all_recommendations = []
            all_reasons = []
            algorithm_contributions = {}
            
            # 1. Content-based recommendations (40%)
            content_limit = int(total_limit * self.weights['content_based'])
            content_books, content_reasons = self.content_based_recommendations(profile, content_limit)
            all_recommendations.extend(content_books)
            all_reasons.extend(content_reasons)
            algorithm_contributions['content_based'] = len(content_books)
            
            # 2. Collaborative filtering (30%)
            collab_limit = int(total_limit * self.weights['collaborative'])
            collab_books, collab_reasons = self.collaborative_filtering_recommendations(profile, collab_limit)
            # Remove duplicates
            collab_books = [book for book in collab_books 
                          if book.id not in {b.id for b in all_recommendations}]
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
        """Cache recommendations for future use"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Prepare data for caching
            import json
            cache_data = {
                'books': [book.__dict__ for book in result.books],
                'confidence_score': result.confidence_score,
                'reasons': result.reasons
            }
            
            # Cache expires in 24 hours
            expires_at = datetime.now() + timedelta(hours=24)
            
            cursor.execute('''
                INSERT OR REPLACE INTO user_recommendations 
                (user_id, recommendations, algorithm_used, expires_at)
                VALUES (?, ?, ?, ?)
            ''', (user_id, json.dumps(cache_data), result.algorithm_used, expires_at))
            
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

# Flask API Integration Classes
class RecommendationAPI:
    """Flask API endpoints for the recommendation system"""

    def __init__(self, recommendation_engine: BookRecommendationEngine):
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

    def get_similar_books(self, book_id: int, user_id: str = None, limit: int = 10) -> Dict:
        """
        Get books similar to a given book, optionally personalized to a user.
        Uses the engine's _get_book_from_main_db and _calculate_content_score.
        """
        try:
            # Get the reference book
            book = self.engine._get_book_from_main_db(book_id)
            if not book:
                return {'success': False, 'error': 'Book not found', 'books': []}

            # Get user profile if user_id provided, else use empty profile
            if user_id:
                profile = self.engine.get_user_profile(user_id)
            else:
                # Empty profile
                profile = UserProfile(user_id="anon", email="", selected_genres=[])

            # Use the genre of the book to fetch similar books
            genre = book.genre or ""
            similar_books = self.engine._fetch_books_by_genre(genre, limit*2, exclude_ids=[book.id])
            # Optionally, score by content similarity
            scored_books = []
            for b in similar_books:
                score = self.engine._calculate_content_score(b, profile)
                b.content_score = score
                scored_books.append(b)
            # Sort and take top
            scored_books.sort(key=lambda x: getattr(x, 'content_score', 0), reverse=True)
            top_books = scored_books[:limit]
            return {
                'success': True,
                'books': [b.__dict__ for b in top_books]
            }
        except Exception as e:
            logger.error(f"Error in get_similar_books: {e}")
            return {'success': False, 'error': str(e), 'books': []}

    def get_recommendations_by_genre(self, genres: List[str], user_id: str = None, limit: int = 20) -> Dict:
        """
        Get recommendations for a list of genres, optionally personalized to a user.
        Uses the engine's _fetch_books_by_genre and _calculate_content_score.
        """
        try:
            exclude_ids = []
            if user_id:
                profile = self.engine.get_user_profile(user_id)
                exclude_ids = profile.reading_history + profile.wishlist
            else:
                # Empty profile
                profile = UserProfile(user_id="anon", email="", selected_genres=[])

            # Fetch and score books for each genre
            books = []
            genres = genres or []
            per_genre = max(1, limit // max(1, len(genres)))
            for genre in genres:
                genre_books = self.engine._fetch_books_by_genre(genre, per_genre*2, exclude_ids)
                for b in genre_books:
                    score = self.engine._calculate_content_score(b, profile)
                    b.content_score = score
                # Sort and take top per genre
                genre_books.sort(key=lambda x: getattr(x, 'content_score', 0), reverse=True)
                books.extend(genre_books[:per_genre])
                if len(books) >= limit:
                    break
            # Remove duplicates
            seen = set()
            unique_books = []
            for b in books:
                if b.id not in seen:
                    seen.add(b.id)
                    unique_books.append(b)
                if len(unique_books) >= limit:
                    break
            return {
                'success': True,
                'books': [b.__dict__ for b in unique_books]
            }
        except Exception as e:
            logger.error(f"Error in get_recommendations_by_genre: {e}")
            return {'success': False, 'error': str(e), 'books': []}

    def get_search_based_recommendations(self, query: str, user_id: str = None, limit: int = 20) -> Dict:
        """
        Get recommendations based on a search query, optionally personalized to a user.
        Uses the engine's _search_books_by_query and _calculate_content_score.
        """
        try:
            exclude_ids = []
            if user_id:
                profile = self.engine.get_user_profile(user_id)
                exclude_ids = profile.reading_history + profile.wishlist
            else:
                profile = UserProfile(user_id="anon", email="", selected_genres=[])

            books = self.engine._search_books_by_query(query, limit*2, exclude_ids)
            for b in books:
                score = self.engine._calculate_content_score(b, profile)
                b.content_score = score
            books.sort(key=lambda x: getattr(x, 'content_score', 0), reverse=True)
            top_books = books[:limit]
            return {
                'success': True,
                'books': [b.__dict__ for b in top_books]
            }
        except Exception as e:
            logger.error(f"Error in get_search_based_recommendations: {e}")
            return {'success': False, 'error': str(e), 'books': []}

# Test function to verify database integration
def test_recommendation_system_with_db():
    """Test the recommendation system with database integration"""
    
    # Initialize engine
    engine = BookRecommendationEngine()
    
    # Test user
    test_user_id = "test_user_123"
    test_genres = ["Science Fiction", "Fantasy", "Technology"]
    
    print("Testing Database Integration...")
    
    # Test fetching trending books
    print("\n1. Testing trending books fetch:")
    trending = engine._fetch_trending_books(5)
    print(f"Fetched {len(trending)} trending books")
    for book in trending[:3]:
        print(f"  - {book.title} by {book.author} (Rating: {book.rating})")
    
    # Test fetching books by genre
    print("\n2. Testing genre-based fetch:")
    sci_fi_books = engine._fetch_books_by_genre("Science Fiction", 5)
    print(f"Fetched {len(sci_fi_books)} sci-fi books")
    for book in sci_fi_books[:3]:
        print(f"  - {book.title} by {book.author}")
    
    # Test search functionality
    print("\n3. Testing search functionality:")
    search_results = engine._search_books_by_query("fiction", 5)
    print(f"Fetched {len(search_results)} books for 'fiction'")
    for book in search_results[:3]:
        print(f"  - {book.title} by {book.author}")
    
    # Record some sample interactions
    print("\n4. Recording sample interactions:")
    sample_interactions = [
        (1, "view", 1.0),
        (2, "wishlist_add", 2.0),
        (3, "view", 1.0),
        (4, "wishlist_add", 2.0),
        (5, "search", 0.5)
    ]
    
    for book_id, interaction_type, weight in sample_interactions:
        engine.record_user_interaction(test_user_id, book_id, interaction_type, weight)
        print(f"  Recorded: {interaction_type} for book {book_id}")
    
    # Get recommendations
    print("\n5. Generating recommendations:")
    result = engine.get_recommendations_for_user(
        user_id=test_user_id,
        email="test@example.com",
        selected_genres=test_genres,
        limit=10
    )
    
    print(f"\nRecommendation Results:")
    print(f"Algorithm: {result.algorithm_used}")
    print(f"Confidence: {result.confidence_score:.2f}")
    print(f"Books found: {len(result.books)}")
    print(f"Reasons: {result.reasons}")
    
    print(f"\nTop recommendations:")
    for i, book in enumerate(result.books[:5], 1):
        print(f"{i}. {book.title} by {book.author}")
        if book.genre:
            print(f"   Genre: {book.genre}")
        if book.rating:
            print(f"   Rating: {book.rating}")
    
    # Test personalized trending
    print("\n6. Testing personalized trending:")
    trending_result = engine.get_personalized_trending(test_user_id, limit=5)
    print(f"Algorithm: {trending_result.algorithm_used}")
    print(f"Confidence: {trending_result.confidence_score:.2f}")
    print(f"Books found: {len(trending_result.books)}")
    
    # Get stats
    stats = engine.get_recommendation_stats(test_user_id)
    print(f"\n7. User Stats: {stats}")
    
    return result

if __name__ == "__main__":
    # Test the system
    print("Testing Book Recommendation System with Database Integration...")
    try:
        test_result = test_recommendation_system_with_db()
        
        print("""
        
        Database-Integrated Recommendation System Ready!
        
        Key Features:
        ✅ Direct PostgreSQL database integration
        ✅ Uses existing database functions (no API calls)
        ✅ Hybrid recommendation algorithms
        ✅ User interaction tracking
        ✅ Personalized recommendations
        ✅ Trending book integration
        ✅ Caching for performance
        ✅ Fallback mechanisms
        
        Integration Instructions:
        1. Add the recommendation routes to your Flask app
        2. Import and initialize the recommendation engine
        3. Record user interactions when they browse books
        4. Use the API to serve personalized recommendations
        
        Database Dependencies:
        - Uses your existing PostgreSQL connection
        - Creates separate SQLite database for recommendation data
        - Integrates with user_interests table
        """)
        
    except Exception as e:
        print(f"Error during testing: {e}")
        print("Please ensure your database connection is properly configured.")