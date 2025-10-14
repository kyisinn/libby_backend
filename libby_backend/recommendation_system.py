import logging
import random
import re
import math
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Set
from dataclasses import dataclass
from collections import defaultdict, Counter
from decimal import Decimal

from libby_backend.database import (
    get_db_connection,
    get_book_by_id_db,
    get_trending_books_db,
    count_user_interactions,
    count_user_interests,
)
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def safe_float_conversion(value):
    """Safely convert value to float, handling Decimal/None"""
    if value is None:
        return None
    try:
        if isinstance(value, Decimal):
            return float(value)
        return float(value) if value else None
    except (TypeError, ValueError, AttributeError):
        return None


@dataclass
class Book:
    """Book data structure"""
    id: int
    title: str
    author: str
    genre: Optional[str] = None
    description: Optional[str] = None
    cover_image_url: Optional[str] = None
    rating: Optional[float] = None
    publication_date: Optional[str] = None
    isbn: Optional[str] = None
    similarity_score: Optional[float] = None

    def __post_init__(self):
        if self.rating is not None:
            self.rating = safe_float_conversion(self.rating)


@dataclass
class UserProfile:
    """User profile with preferences"""
    user_id: str
    email: str
    selected_genres: List[str]
    clerk_user_id: Optional[str] = None
    reading_history: List[int] = None
    wishlist: List[int] = None
    interaction_weights: Dict[str, float] = None
    favorite_authors: List[str] = None
    preferred_rating_threshold: float = 3.5

    def __post_init__(self):
        if self.reading_history is None:
            self.reading_history = []
        if self.wishlist is None:
            self.wishlist = []
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
    contributions: Optional[Dict[str, int]] = None
    interaction_count: Optional[int] = 0


class TFIDFVectorizer:
    """Simple TF-IDF implementation for book content"""
    
    def __init__(self):
        self.vocabulary = {}
        self.idf_scores = {}
        self.document_count = 0
    
    def tokenize(self, text: str) -> List[str]:
        """Simple tokenization"""
        if not text:
            return []
        # Convert to lowercase and split on non-alphanumeric
        tokens = re.findall(r'\b\w+\b', text.lower())
        # Remove common stopwords
        stopwords = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'from', 'as'}
        return [t for t in tokens if t not in stopwords and len(t) > 2]
    
    def build_vocabulary(self, documents: List[str]):
        """Build vocabulary and compute IDF scores"""
        doc_freq = Counter()
        self.document_count = len(documents)
        
        for doc in documents:
            unique_tokens = set(self.tokenize(doc))
            for token in unique_tokens:
                doc_freq[token] += 1
        
        # Build vocabulary and IDF scores
        for idx, (token, freq) in enumerate(doc_freq.items()):
            self.vocabulary[token] = idx
            # IDF = log(N / df)
            self.idf_scores[token] = math.log(self.document_count / freq) if freq > 0 else 0
    
    def vectorize(self, text: str) -> Dict[int, float]:
        """Convert text to TF-IDF vector (sparse representation)"""
        tokens = self.tokenize(text)
        if not tokens:
            return {}
        
        # Calculate term frequency
        tf = Counter(tokens)
        doc_length = len(tokens)
        
        # Build TF-IDF vector
        vector = {}
        for token, count in tf.items():
            if token in self.vocabulary:
                idx = self.vocabulary[token]
                tf_score = count / doc_length  # Normalized TF
                idf_score = self.idf_scores.get(token, 0)
                vector[idx] = tf_score * idf_score
        
        return vector
    
    @staticmethod
    def cosine_similarity(vec1: Dict[int, float], vec2: Dict[int, float]) -> float:
        """Compute cosine similarity between two sparse vectors"""
        if not vec1 or not vec2:
            return 0.0
        
        # Compute dot product
        dot_product = sum(vec1.get(k, 0) * vec2.get(k, 0) for k in set(vec1.keys()) | set(vec2.keys()))
        
        # Compute magnitudes
        mag1 = math.sqrt(sum(v * v for v in vec1.values()))
        mag2 = math.sqrt(sum(v * v for v in vec2.values()))
        
        if mag1 == 0 or mag2 == 0:
            return 0.0
        
        return dot_product / (mag1 * mag2)


class EnhancedBookRecommendationEngine:
    """Enhanced book recommendation engine with mathematical algorithms"""

    def __init__(self):
        # Algorithm weights (as per specification)
        self.weights = {
            'content_based': 0.35,
            'collaborative': 0.25,
            'trending': 0.20,
            'author_based': 0.15,
            'diversity': 0.05
        }
        
        self.tfidf = TFIDFVectorizer()
        self.book_vectors = {}  # Cache for book TF-IDF vectors
        self.user_rating_matrix = {}  # user_id -> {book_id -> rating}

    def _get_book_content(self, book: Book) -> str:
        """Extract content for TF-IDF vectorization"""
        parts = []
        if book.title:
            parts.append(book.title * 2)  # Weight title higher
        if book.genre:
            parts.append(book.genre * 2)  # Weight genre higher
        if book.description:
            parts.append(book.description)
        if book.author:
            parts.append(book.author)
        return ' '.join(parts)

    def _build_tfidf_model(self, books: List[Book]):
        """Build TF-IDF model from books"""
        documents = [self._get_book_content(book) for book in books]
        self.tfidf.build_vocabulary(documents)
        
        # Vectorize all books
        for book in books:
            content = self._get_book_content(book)
            self.book_vectors[book.id] = self.tfidf.vectorize(content)

    def _load_user_rating_matrix(self, profile: UserProfile):
        """Load user-book rating matrix from database"""
        try:
            conn = get_db_connection()
            if not conn:
                return
            
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get ratings for all users who have rated books
                cur.execute("""
                    SELECT clerk_user_id, book_id, rating
                    FROM user_interactions
                    WHERE rating IS NOT NULL AND rating > 0
                """)
                rows = cur.fetchall()
                
                for row in rows:
                    user_id = row['clerk_user_id']
                    book_id = row['book_id']
                    rating = float(row['rating'])
                    
                    if user_id not in self.user_rating_matrix:
                        self.user_rating_matrix[user_id] = {}
                    self.user_rating_matrix[user_id][book_id] = rating
            
            conn.close()
            logger.info(f"Loaded rating matrix: {len(self.user_rating_matrix)} users")
            
        except Exception as e:
            logger.error(f"Error loading rating matrix: {e}")

    def _pearson_correlation(self, user1_ratings: Dict[int, float], user2_ratings: Dict[int, float]) -> float:
        """
        Compute Pearson Correlation Coefficient between two users
        Formula: r_xy = Σ(R_x,i - R̄_x)(R_y,i - R̄_y) / sqrt(Σ(R_x,i - R̄_x)²) * sqrt(Σ(R_y,i - R̄_y)²)
        """
        # Find common books
        common_books = set(user1_ratings.keys()) & set(user2_ratings.keys())
        if len(common_books) < 2:  # Need at least 2 common items
            return 0.0
        
        # Calculate means
        mean1 = sum(user1_ratings[b] for b in common_books) / len(common_books)
        mean2 = sum(user2_ratings[b] for b in common_books) / len(common_books)
        
        # Calculate Pearson correlation
        numerator = sum((user1_ratings[b] - mean1) * (user2_ratings[b] - mean2) for b in common_books)
        
        sum_sq1 = sum((user1_ratings[b] - mean1) ** 2 for b in common_books)
        sum_sq2 = sum((user2_ratings[b] - mean2) ** 2 for b in common_books)
        
        denominator = math.sqrt(sum_sq1) * math.sqrt(sum_sq2)
        
        if denominator == 0:
            return 0.0
        
        return numerator / denominator

    def _predict_rating_collaborative(self, target_user: str, book_id: int, k: int = 10) -> float:
        """
        Predict rating using collaborative filtering with Pearson correlation
        Formula: R̂_u,j = R̄_u + Σ(r_u,n * (R_n,j - R̄_n)) / Σ|r_u,n|
        """
        if target_user not in self.user_rating_matrix:
            return 0.0
        
        target_ratings = self.user_rating_matrix[target_user]
        target_mean = sum(target_ratings.values()) / len(target_ratings) if target_ratings else 0
        
        # Find similar users who have rated this book
        similarities = []
        for other_user, other_ratings in self.user_rating_matrix.items():
            if other_user == target_user or book_id not in other_ratings:
                continue
            
            similarity = self._pearson_correlation(target_ratings, other_ratings)
            if similarity > 0:  # Only consider positive correlations
                similarities.append((other_user, similarity))
        
        if not similarities:
            return 0.0
        
        # Get top-K similar users
        similarities.sort(key=lambda x: x[1], reverse=True)
        top_k = similarities[:k]
        
        # Predict rating
        numerator = 0.0
        denominator = 0.0
        
        for other_user, similarity in top_k:
            other_ratings = self.user_rating_matrix[other_user]
            other_mean = sum(other_ratings.values()) / len(other_ratings)
            
            numerator += similarity * (other_ratings[book_id] - other_mean)
            denominator += abs(similarity)
        
        if denominator == 0:
            return target_mean
        
        predicted = target_mean + (numerator / denominator)
        return max(0.0, min(5.0, predicted))  # Clamp to [0, 5]

    def get_books_by_genre(self, genre: str, limit: int = 20, exclude_ids: List[int] = None) -> List[Book]:
        """Get books by genre"""
        try:
            if exclude_ids is None:
                exclude_ids = []

            conn = get_db_connection()
            if not conn:
                return []

            books = []
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                params = [f'%{genre.lower()}%']
                exclude_clause = ""
                if exclude_ids:
                    placeholders = ','.join(['%s'] * len(exclude_ids))
                    exclude_clause = f"AND book_id NOT IN ({placeholders})"
                    params.extend(exclude_ids)
                params.append(limit)

                cursor.execute(f"""
                    SELECT book_id, title, author, genre, description, cover_image_url,
                           rating, publication_date, isbn
                    FROM books
                    WHERE LOWER(genre) LIKE %s {exclude_clause}
                      AND title IS NOT NULL
                      AND author IS NOT NULL
                    ORDER BY rating DESC NULLS LAST, book_id
                    LIMIT %s
                """, params)

                for row in cursor.fetchall():
                    book_id = row.get("book_id")
                    if book_id and book_id not in exclude_ids:
                        books.append(Book(
                            id=book_id,
                            title=row.get("title") or "",
                            author=row.get("author") or "Unknown Author",
                            genre=row.get("genre"),
                            description=row.get("description"),
                            cover_image_url=row.get("cover_image_url"),
                            rating=safe_float_conversion(row.get("rating")),
                            publication_date=str(row.get("publication_date")) if row.get("publication_date") else None,
                            isbn=row.get("isbn")
                        ))
                        exclude_ids.append(book_id)

            conn.close()
            return books[:limit]

        except Exception as e:
            logger.error(f"Error in genre search: {e}")
            return []

    def get_books_by_author(self, author_name: str, limit: int = 10, exclude_ids: List[int] = None) -> List[Book]:
        """Get books by specific author"""
        try:
            exclude_ids = exclude_ids or []
            conn = get_db_connection()
            if not conn:
                return []

            books = []
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                params = [f'%{author_name.lower()}%']
                exclude_clause = ""
                if exclude_ids:
                    placeholders = ','.join(['%s'] * len(exclude_ids))
                    exclude_clause = f"AND book_id NOT IN ({placeholders})"
                    params.extend(exclude_ids)
                params.append(limit)

                cursor.execute(f"""
                    SELECT book_id, title, author, genre, description, cover_image_url,
                           rating, publication_date, isbn
                    FROM books
                    WHERE LOWER(author) LIKE %s {exclude_clause}
                    ORDER BY rating DESC NULLS LAST
                    LIMIT %s
                """, params)

                for row in cursor.fetchall():
                    books.append(Book(
                        id=row["book_id"],
                        title=row["title"],
                        author=row["author"],
                        genre=row.get("genre"),
                        description=row.get("description"),
                        cover_image_url=row.get("cover_image_url"),
                        rating=safe_float_conversion(row.get("rating")),
                        publication_date=str(row.get("publication_date")) if row.get("publication_date") else None,
                        isbn=row.get("isbn")
                    ))

            conn.close()
            return books

        except Exception as e:
            logger.error(f"Error getting books by author: {e}")
            return []

    def content_based_filtering(self, profile: UserProfile, limit: int = 15, all_books: List[Book] = None) -> Tuple[List[Book], List[str]]:
        """
        Content-Based Filtering with TF-IDF and Cosine Similarity
        Steps:
        1. Build TF-IDF vectors for all books
        2. Create user profile vector (weighted average of interacted books)
        3. Compute cosine similarity
        4. Rank and return top-N
        """
        recommendations = []
        reasons = []
        
        try:
            # Load all books if not provided
            if all_books is None:
                all_books = self._get_all_books_sample(limit * 10)
            
            if not all_books:
                return [], []
            
            # Build TF-IDF model
            self._build_tfidf_model(all_books)
            
            # Get user's interacted books
            user_books = self._get_user_interacted_books(profile)
            
            if not user_books:
                # New user: use genre preferences
                return self._content_based_by_genre(profile, limit, all_books)
            
            # Build user profile vector (weighted average)
            user_vector = {}
            total_weight = 0.0
            
            for book_id, weight in user_books.items():
                if book_id in self.book_vectors:
                    book_vec = self.book_vectors[book_id]
                    for idx, val in book_vec.items():
                        user_vector[idx] = user_vector.get(idx, 0.0) + (val * weight)
                    total_weight += weight
            
            # Normalize user vector
            if total_weight > 0:
                user_vector = {k: v / total_weight for k, v in user_vector.items()}
            
            # Compute similarity for all books
            scored_books = []
            exclude_ids = set(profile.reading_history + profile.wishlist)
            
            for book in all_books:
                if book.id in exclude_ids:
                    continue
                
                if book.id in self.book_vectors:
                    similarity = self.tfidf.cosine_similarity(user_vector, self.book_vectors[book.id])
                    if similarity > 0.1:  # Threshold
                        book.similarity_score = similarity
                        scored_books.append(book)
            
            # Sort by similarity and take top-N
            scored_books.sort(key=lambda x: x.similarity_score, reverse=True)
            recommendations = scored_books[:limit]
            
            if recommendations:
                reasons.append("Based on content similarity to your reading history")
                reasons.append(f"Using TF-IDF vectorization and cosine similarity")
            
        except Exception as e:
            logger.error(f"Error in content-based filtering: {e}")
        
        return recommendations, reasons

    def _content_based_by_genre(self, profile: UserProfile, limit: int, all_books: List[Book]) -> Tuple[List[Book], List[str]]:
        """Fallback content-based for new users based on genre preferences"""
        recommendations = []
        reasons = []
        
        if not profile.selected_genres:
            return [], []
        
        exclude_ids = set(profile.reading_history + profile.wishlist)
        
        for book in all_books:
            if book.id in exclude_ids:
                continue
            
            if book.genre:
                for user_genre in profile.selected_genres:
                    if user_genre.lower() in book.genre.lower():
                        # Simple scoring based on genre match and rating
                        score = 0.7  # Base genre match score
                        if book.rating and book.rating >= profile.preferred_rating_threshold:
                            score += 0.3 * (book.rating / 5.0)
                        book.similarity_score = score
                        recommendations.append(book)
                        break
        
        recommendations.sort(key=lambda x: getattr(x, 'similarity_score', 0), reverse=True)
        reasons.append(f"Based on your genre preferences: {', '.join(profile.selected_genres)}")
        
        return recommendations[:limit], reasons

    def _get_user_interacted_books(self, profile: UserProfile) -> Dict[int, float]:
        """Get user's interacted books with weights"""
        try:
            conn = get_db_connection()
            if not conn:
                return {}
            
            weights = {}
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT book_id, interaction_type, rating
                    FROM user_interactions
                    WHERE clerk_user_id = %s
                    ORDER BY timestamp DESC
                    LIMIT 50
                """, (profile.clerk_user_id,))
                
                rows = cur.fetchall()
                
                # Weight mapping
                type_weights = {
                    'rate': 3.0,
                    'wishlist_add': 2.0,
                    'like': 2.0,
                    'view': 1.0,
                    'search': 0.5
                }
                
                for row in rows:
                    book_id = row['book_id']
                    itype = row['interaction_type']
                    rating = row.get('rating')
                    
                    weight = type_weights.get(itype, 1.0)
                    if rating:
                        weight *= (float(rating) / 5.0)  # Scale by rating
                    
                    weights[book_id] = weights.get(book_id, 0) + weight
            
            conn.close()
            return weights
            
        except Exception as e:
            logger.error(f"Error getting user interactions: {e}")
            return {}

    def _get_all_books_sample(self, limit: int = 500) -> List[Book]:
        """Get sample of all books for recommendation"""
        try:
            conn = get_db_connection()
            if not conn:
                return []
            
            books = []
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT book_id, title, author, genre, description, cover_image_url,
                           rating, publication_date, isbn
                    FROM books
                    WHERE title IS NOT NULL AND author IS NOT NULL
                    ORDER BY rating DESC NULLS LAST
                    LIMIT %s
                """, (limit,))
                
                for row in cur.fetchall():
                    books.append(Book(
                        id=row["book_id"],
                        title=row["title"],
                        author=row["author"],
                        genre=row.get("genre"),
                        description=row.get("description"),
                        cover_image_url=row.get("cover_image_url"),
                        rating=safe_float_conversion(row.get("rating")),
                        publication_date=str(row.get("publication_date")) if row.get("publication_date") else None,
                        isbn=row.get("isbn")
                    ))
            
            conn.close()
            return books
            
        except Exception as e:
            logger.error(f"Error getting books sample: {e}")
            return []

    def collaborative_filtering(self, profile: UserProfile, limit: int = 10) -> Tuple[List[Book], List[str]]:
        """
        Collaborative Filtering using implicit feedback (views, likes, wishlist, ratings)
        Uses the database function that finds similar users based on interaction overlap
        """
        recommendations = []
        reasons = []
        
        try:
            # Use the database collaborative filtering that handles implicit feedback
            from libby_backend.database import collaborative_filtering_recommendations_pg
            
            # Resolve user_id (may be None)
            user_id = None
            if profile.user_id and str(profile.user_id).isdigit():
                user_id = int(profile.user_id)
            
            rows, db_reasons = collaborative_filtering_recommendations_pg(
                clerk_user_id=profile.clerk_user_id,
                user_id=user_id,
                limit=limit
            )
            
            if rows:
                for row in rows:
                    book = Book(
                        id=row['book_id'],
                        title=row.get('title', 'Unknown'),
                        author=row.get('author', 'Unknown'),
                        genre=row.get('genre'),
                        cover_image_url=row.get('cover_image_url'),
                        rating=safe_float_conversion(row.get('rating')),
                        similarity_score=float(row.get('score', 0.5))
                    )
                    recommendations.append(book)
                
                reasons.extend(db_reasons)
                if not reasons:
                    reasons.append("Based on users with similar reading preferences")
                    
                logger.info(f"Collaborative filtering returned {len(recommendations)} books")
            else:
                # Fallback: Load rating matrix for explicit ratings (legacy)
                self._load_user_rating_matrix(profile)
                logger.info(f"Loaded rating matrix: {len(self.user_rating_matrix)} users with explicit ratings")
            
        except Exception as e:
            logger.error(f"Error in collaborative filtering: {e}")
        
        return recommendations, reasons

    def _fetch_books_by_ids(self, book_ids: List[int]) -> List[Book]:
        """Fetch books by their IDs"""
        try:
            conn = get_db_connection()
            if not conn or not book_ids:
                return []
            
            books = []
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                placeholders = ','.join(['%s'] * len(book_ids))
                cur.execute(f"""
                    SELECT book_id, title, author, genre, description, cover_image_url,
                           rating, publication_date, isbn
                    FROM books
                    WHERE book_id IN ({placeholders})
                """, book_ids)
                
                for row in cur.fetchall():
                    books.append(Book(
                        id=row["book_id"],
                        title=row["title"],
                        author=row["author"],
                        genre=row.get("genre"),
                        description=row.get("description"),
                        cover_image_url=row.get("cover_image_url"),
                        rating=safe_float_conversion(row.get("rating")),
                        publication_date=str(row.get("publication_date")) if row.get("publication_date") else None,
                        isbn=row.get("isbn")
                    ))
            
            conn.close()
            return books
            
        except Exception as e:
            logger.error(f"Error fetching books by IDs: {e}")
            return []

    def trending_recommendations(self, limit: int, exclude_ids: List[int], profile: UserProfile) -> List[Book]:
        """
        Trending recommendations with popularity score
        Formula: P_i = avg_rating_i × log(1 + interaction_count_i)
        """
        try:
            conn = get_db_connection()
            if not conn:
                return []
            
            books = []
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                exclude_clause = ""
                params = []
                if exclude_ids:
                    placeholders = ','.join(['%s'] * len(exclude_ids))
                    exclude_clause = f"AND b.book_id NOT IN ({placeholders})"
                    params.extend(exclude_ids)
                params.append(limit * 2)
                
                # Calculate popularity score
                cur.execute(f"""
                    SELECT b.book_id, b.title, b.author, b.genre, b.description, b.cover_image_url,
                           b.rating, b.publication_date, b.isbn,
                           COUNT(ui.id) as interaction_count
                    FROM books b
                    LEFT JOIN user_interactions ui ON b.book_id = ui.book_id
                    WHERE b.rating IS NOT NULL 
                    AND b.rating >= %s
                    {exclude_clause}
                    GROUP BY b.book_id
                    HAVING COUNT(ui.id) > 0
                    ORDER BY (b.rating * LOG(1 + COUNT(ui.id))) DESC
                    LIMIT %s
                """, [profile.preferred_rating_threshold] + params)
                
                for row in cur.fetchall():
                    book = Book(
                        id=row["book_id"],
                        title=row["title"],
                        author=row["author"],
                        genre=row.get("genre"),
                        description=row.get("description"),
                        cover_image_url=row.get("cover_image_url"),
                        rating=safe_float_conversion(row.get("rating")),
                        publication_date=str(row.get("publication_date")) if row.get("publication_date") else None,
                        isbn=row.get("isbn")
                    )
                    
                    # Calculate normalized popularity score
                    rating = book.rating or 3.0
                    interaction_count = row['interaction_count']
                    popularity = rating * math.log(1 + interaction_count)
                    book.similarity_score = min(popularity / 20.0, 1.0)  # Normalize
                    
                    # Boost for matching preferences
                    if book.genre and any(g.lower() in book.genre.lower() for g in profile.selected_genres):
                        book.similarity_score = min(book.similarity_score * 1.3, 1.0)
                    
                    books.append(book)
            
            conn.close()
            return books[:limit]
            
        except Exception as e:
            logger.error(f"Error in trending recommendations: {e}")
            return []

    def author_based_filtering(self, profile: UserProfile, limit: int = 10) -> Tuple[List[Book], List[str]]:
        """
        Author-based recommendations
        Steps:
        1. Identify user's top authors
        2. Recommend unread books by those authors
        3. Expand to similar authors if needed
        """
        recommendations = []
        reasons = []
        exclude_ids = set(profile.reading_history + profile.wishlist)
        
        try:
            # Get top authors from interactions
            top_authors = self._get_top_authors(profile)
            
            if not top_authors:
                top_authors = profile.favorite_authors[:5]
            
            if not top_authors:
                return [], []
            
            # Get books by top authors
            for author in top_authors:
                author_books = self.get_books_by_author(author, limit // len(top_authors) + 2, list(exclude_ids))
                for book in author_books:
                    if book.id not in exclude_ids:
                        book.similarity_score = 0.8  # High score for favorite authors
                        recommendations.append(book)
                        exclude_ids.add(book.id)
                
                if author_books:
                    reasons.append(f"Books by {author}")
                
                if len(recommendations) >= limit:
                    break
            
        except Exception as e:
            logger.error(f"Error in author-based filtering: {e}")
        
        return recommendations[:limit], reasons

    def _get_top_authors(self, profile: UserProfile, top_n: int = 5) -> List[str]:
        """Get user's most interacted authors"""
        try:
            conn = get_db_connection()
            if not conn:
                return []
            
            authors = []
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT b.author, COUNT(*) as count
                    FROM user_interactions ui
                    JOIN books b ON ui.book_id = b.book_id
                    WHERE ui.clerk_user_id = %s
                    AND ui.interaction_type IN ('rate', 'wishlist_add', 'like')
                    GROUP BY b.author
                    ORDER BY count DESC
                    LIMIT %s
                """, (profile.clerk_user_id, top_n))
                
                authors = [row['author'] for row in cur.fetchall()]
            
            conn.close()
            return authors
            
        except Exception as e:
            logger.error(f"Error getting top authors: {e}")
            return []

    def diversity_injection(self, profile: UserProfile, limit: int, exclude_ids: Set[int]) -> List[Book]:
        """
        Diversity injection to prevent overspecialization
        Steps:
        1. Identify user's top-N genres
        2. Select high-rated books from unexplored genres
        3. Maintain quality threshold
        """
        try:
            if limit <= 0:
                return []
            
            # Get available genres
            conn = get_db_connection()
            if not conn:
                return []
            
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT DISTINCT LOWER(genre) as genre_lower
                    FROM books 
                    WHERE genre IS NOT NULL AND genre != ''
                    LIMIT 50
                """)
                available_genres = [row['genre_lower'] for row in cur.fetchall()]
            conn.close()
            
            if not available_genres:
                return []
            
            # Find unexplored genres
            explored = {g.lower() for g in profile.selected_genres}
            explored.update(profile.interaction_weights.keys())
            unexplored = [g for g in available_genres if g not in explored]
            
            if not unexplored:
                unexplored = random.sample(available_genres, min(3, len(available_genres)))
            
            # Select genres
            selected = random.sample(unexplored, min(3, len(unexplored)))
            if not selected:
                return []
            
            diversity_books = []
            books_per_genre = max(1, limit // len(selected))
            
            for genre in selected:
                genre_books = self.get_books_by_genre(genre, books_per_genre, list(exclude_ids))
                for book in genre_books:
                    # Only add high-rated books for diversity
                    if book.rating and book.rating >= profile.preferred_rating_threshold:
                        if book.id not in exclude_ids:
                            book.similarity_score = 0.4  # Lower score for diversity
                            diversity_books.append(book)
                            exclude_ids.add(book.id)
                
                if len(diversity_books) >= limit:
                    break
            
            return diversity_books[:limit]
            
        except Exception as e:
            logger.error(f"Error in diversity injection: {e}")
            return []

    def hybrid_recommendations_enhanced(self, profile: UserProfile, total_limit: int = 30) -> RecommendationResult:
        """
        Hybrid Fusion with mathematical weighting
        Formula: Final_Score = 0.35(CBF) + 0.25(CF) + 0.20(Trending) + 0.15(Author) + 0.05(Diversity)
        
        Post-processing:
        - Remove duplicates
        - Normalize scores (0-1)
        - Apply rating threshold
        - Return Top-N
        """
        try:
            # Hydrate profile from DB
            try:
                self._hydrate_profile_from_db(profile)
            except Exception:
                pass

            all_reasons = []
            per_source = {}
            
            # Get sample of all books for CBF
            all_books = self._get_all_books_sample(500)

            # 1. Content-Based Filtering (35%)
            content_limit = int(total_limit * self.weights['content_based'])
            content_books, content_reasons = self.content_based_filtering(profile, content_limit, all_books)
            per_source["content"] = content_books
            all_reasons += content_reasons

            # 2. Collaborative Filtering (25%)
            collab_limit = int(total_limit * self.weights['collaborative'])
            collab_books, collab_reasons = self.collaborative_filtering(profile, collab_limit)
            per_source["collab"] = collab_books
            all_reasons += collab_reasons

            # 3. Author-Based (15%)
            author_limit = int(total_limit * self.weights['author_based'])
            author_books, author_reasons = self.author_based_filtering(profile, author_limit)
            per_source["author"] = author_books
            all_reasons += author_reasons

            # 4. Trending (20%)
            already_ids = [b.id for src in ("content", "author", "collab") for b in per_source.get(src, [])]
            exclude_ids = already_ids + (profile.reading_history or []) + (profile.wishlist or [])
            trending_limit = int(total_limit * self.weights['trending'])
            trending_books = self.trending_recommendations(trending_limit, exclude_ids, profile)
            per_source["trending"] = trending_books
            if trending_books:
                all_reasons.append("Popular books with high engagement")

            # 5. Diversity (5%)
            diversity_limit = int(total_limit * self.weights['diversity'])
            diversity_books = self.diversity_injection(profile, diversity_limit, set([b.id for b in trending_books] + already_ids))
            per_source["diversity"] = diversity_books
            if diversity_books:
                all_reasons.append("Diverse books from unexplored genres")

            # Hybrid Fusion: Combine with weights
            bookmap = {}
            
            for src, books in per_source.items():
                weight = self.weights.get(f"{src}_based" if src != "trending" and src != "diversity" else src, 0.1)
                
                for b in books:
                    # Ensure all books have a similarity score
                    if not hasattr(b, "similarity_score") or b.similarity_score is None:
                        b.similarity_score = 0.5  # Default
                    
                    bid = int(b.id)
                    
                    # Normalize score to 0-1 range
                    normalized_score = float(b.similarity_score)
                    
                    if bid not in bookmap:
                        bookmap[bid] = {
                            "book": b,
                            "score": normalized_score * weight,
                            "srcs": {src}
                        }
                    else:
                        # If book appears in multiple sources, take max score and add bonus
                        current_score = normalized_score * weight
                        bookmap[bid]["score"] = max(bookmap[bid]["score"], current_score)
                        bookmap[bid]["score"] += 0.1  # Bonus for appearing in multiple sources
                        bookmap[bid]["srcs"].add(src)

            # Sort by final score
            ranked = sorted(bookmap.values(), key=lambda e: e["score"], reverse=True)
            final = [e["book"] for e in ranked]

            # Apply quality filter
            quality = [b for b in final if (getattr(b, 'rating', None) is None) or (b.rating >= profile.preferred_rating_threshold)]
            
            # Top-up if needed
            needed = total_limit - len(quality)
            if needed > 0:
                extra = self.trending_recommendations(needed, [x.id for x in quality], profile)
                quality.extend(extra)
            
            final = quality[:total_limit]

            # Calculate contributions
            contributions = {"content": 0, "author": 0, "collab": 0, "trending": 0, "diversity": 0}
            chosen_ids = {int(b.id) for b in final}
            for bid in chosen_ids:
                srcs = bookmap.get(bid, {}).get("srcs", set())
                for s in srcs:
                    contributions[s] = contributions.get(s, 0) + 1

            # Calculate confidence score
            i_count = count_user_interactions(getattr(profile, 'clerk_user_id', '') or "")
            u_count = count_user_interests(getattr(profile, 'clerk_user_id', '') or "")
            confidence = self._calculate_confidence_score(profile, contributions, total_limit)
            
            # Determine algorithm label
            algo_label = "Hybrid Fusion (CBF + CF + Trending + Author + Diversity)"
            if i_count == 0 and u_count == 0:
                algo_label = "Trending (New User)"
            elif contributions.get('collab', 0) > 0:
                algo_label = "Hybrid (CF + CBF + Trending)"
            
            # Ensure HTTPS URLs
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
            logger.error(f"Error in hybrid recommendations: {e}")
            # Fallback to trending
            trending = self.trending_recommendations(total_limit, [], profile)
            return RecommendationResult(
                books=trending,
                algorithm_used="Fallback (Trending)",
                confidence_score=0.6,
                reasons=["High-quality trending books as fallback"],
                generated_at=datetime.now(),
                contributions={"trending": len(trending)},
                interaction_count=count_user_interactions(getattr(profile, 'clerk_user_id', '') or ""),
            )

    def _calculate_confidence_score(self, profile: UserProfile, contributions: Dict[str, int], target_limit: int) -> float:
        """
        Calculate confidence score
        Formula: Confidence = 0.35R + 0.25D + 0.20C + 0.20Q
        Where:
        - R = Profile Richness (0-1)
        - D = Algorithm Diversity (0-1)
        - C = Coverage Ratio (0-1)
        - Q = Quality Index (0-1)
        """
        try:
            # R: Profile Richness
            richness = 0.0
            if profile.selected_genres:
                richness += 0.25 * min(len(profile.selected_genres) / 5, 1.0)
            if profile.reading_history:
                richness += 0.25 * min(len(profile.reading_history) / 20, 1.0)
            if profile.favorite_authors:
                richness += 0.25 * min(len(profile.favorite_authors) / 5, 1.0)
            if profile.interaction_weights:
                richness += 0.25 * min(len(profile.interaction_weights) / 5, 1.0)
            
            # D: Algorithm Diversity
            active_algorithms = sum(1 for count in contributions.values() if count > 0)
            diversity = active_algorithms / len(self.weights)
            
            # C: Coverage Ratio
            total_recs = sum(contributions.values())
            coverage = min(total_recs / target_limit, 1.0) if target_limit > 0 else 0.0
            
            # Q: Quality Index
            quality = 0.0
            if contributions.get('content', 0) > 0:
                quality += 0.35
            if contributions.get('collab', 0) > 0:
                quality += 0.35
            if contributions.get('author', 0) > 0:
                quality += 0.15
            if contributions.get('trending', 0) > 0:
                quality += 0.10
            if contributions.get('diversity', 0) > 0:
                quality += 0.05
            
            # Final confidence score
            confidence = 0.35 * richness + 0.25 * diversity + 0.20 * coverage + 0.20 * quality
            
            return min(confidence, 1.0)

        except Exception as e:
            logger.error(f"Error calculating confidence: {e}")
            return 0.7

    def _hydrate_profile_from_db(self, profile: UserProfile):
        """Fill missing genres from Postgres user_interests"""
        try:
            if profile and getattr(profile, 'clerk_user_id', None):
                if not profile.selected_genres or not profile.interaction_weights:
                    conn = get_db_connection()
                    if conn:
                        with conn.cursor(cursor_factory=RealDictCursor) as cur:
                            cur.execute("""
                                SELECT LOWER(genre) AS genre, COUNT(*) AS cnt
                                FROM user_interests
                                WHERE clerk_user_id = %s
                                GROUP BY 1 ORDER BY cnt DESC
                            """, (profile.clerk_user_id,))
                            rows = cur.fetchall()
                            if rows:
                                if not profile.selected_genres:
                                    profile.selected_genres = [r["genre"] for r in rows]
                                if not profile.interaction_weights:
                                    profile.interaction_weights = {r["genre"]: float(r["cnt"]) for r in rows}
                        conn.close()
        except Exception as e:
            logger.debug(f"_hydrate_profile_from_db failed: {e}")