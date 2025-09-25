# libby_backend/recommendation_system.py

import random
from datetime import datetime
from typing import List, Optional, Dict, Any
import math

from libby_backend.database import (
    get_user_interactions,
    get_books_by_genres,
    get_books_by_author,
    get_trending_books,
    get_book_by_id,
    get_all_books,
)

# ------------------------------
# Models
# ------------------------------

class Book:
    def __init__(self, id: int, title: str, author: str,
                 genre: Optional[str] = None,
                 cover_image_url: Optional[str] = None,
                 rating: Optional[float] = None,
                 description: Optional[str] = None,
                 publication_date: Optional[str] = None,
                 pages: Optional[int] = None,
                 language: Optional[str] = None,
                 isbn: Optional[str] = None,
                 similarity_score: Optional[float] = None):
        self.id = id
        self.title = title
        self.author = author
        self.genre = genre
        self.cover_image_url = cover_image_url or f"https://placehold.co/320x480/1e1f22/ffffff?text={title}"
        self.rating = rating or round(random.uniform(2.5, 5.0), 1)
        self.description = description
        self.publication_date = publication_date
        self.pages = pages
        self.language = language
        self.isbn = isbn
        self.similarity_score = similarity_score

    def to_dict(self):
        return {
            "id": self.id,
            "title": self.title,
            "author": self.author,
            "genre": self.genre,
            "description": self.description,
            "cover_image_url": self.cover_image_url,
            "coverurl": self.cover_image_url,  # for frontend compatibility
            "rating": self.rating,
            "publication_date": self.publication_date,
            "pages": self.pages,
            "language": self.language,
            "isbn": self.isbn,
            "similarity_score": self.similarity_score
        }


class RecommendationResult:
    def __init__(self, books: List[Book], algorithm_used: str,
                 confidence_score: float, reasons: List[str],
                 generated_at: Optional[datetime] = None):
        self.books = books
        self.algorithm_used = algorithm_used
        self.confidence_score = confidence_score
        self.reasons = reasons
        self.generated_at = generated_at or datetime.now()

    def to_dict(self):
        return {
            "books": [book.to_dict() for book in self.books],
            "algorithm_used": self.algorithm_used,
            "confidence_score": self.confidence_score,
            "reasons": self.reasons,
            "generated_at": self.generated_at.isoformat(),
        }


# ------------------------------
# Similarity Helpers
# ------------------------------

def cosine_similarity(vec1: Dict[int, float], vec2: Dict[int, float]) -> float:
    """Calculate cosine similarity between two sparse vectors represented as dicts."""
    common_keys = set(vec1.keys()) & set(vec2.keys())
    numerator = sum(vec1[k] * vec2[k] for k in common_keys)
    sum1 = sum(v ** 2 for v in vec1.values())
    sum2 = sum(v ** 2 for v in vec2.values())
    denominator = math.sqrt(sum1) * math.sqrt(sum2)
    if denominator == 0:
        return 0.0
    return numerator / denominator


def pearson_correlation(vec1: Dict[int, float], vec2: Dict[int, float]) -> float:
    """Calculate Pearson correlation coefficient between two sparse vectors represented as dicts."""
    common_keys = set(vec1.keys()) & set(vec2.keys())
    n = len(common_keys)
    if n == 0:
        return 0.0
    sum1 = sum(vec1[k] for k in common_keys)
    sum2 = sum(vec2[k] for k in common_keys)
    sum1_sq = sum(vec1[k] ** 2 for k in common_keys)
    sum2_sq = sum(vec2[k] ** 2 for k in common_keys)
    p_sum = sum(vec1[k] * vec2[k] for k in common_keys)
    numerator = p_sum - (sum1 * sum2 / n)
    denominator = math.sqrt((sum1_sq - sum1 ** 2 / n) * (sum2_sq - sum2 ** 2 / n))
    if denominator == 0:
        return 0.0
    return numerator / denominator


# ------------------------------
# Recommendation Engine
# ------------------------------

class EnhancedBookRecommendationEngine:
    def __init__(self):
        self.user_profiles_cache = {}

    def build_user_profile(self, user_id: str) -> Dict[str, Any]:
        """Build user profile from interactions."""
        interactions = get_user_interactions(user_id)
        genre_counts = {}
        author_counts = {}
        rating_sum = 0.0
        rating_count = 0
        for interaction in interactions:
            book = get_book_by_id(interaction["book_id"])
            if not book:
                continue
            genre_counts[book.genre] = genre_counts.get(book.genre, 0) + 1
            author_counts[book.author] = author_counts.get(book.author, 0) + 1
            if "rating" in interaction and interaction["rating"] is not None:
                rating_sum += interaction["rating"]
                rating_count += 1
        avg_rating = rating_sum / rating_count if rating_count > 0 else None
        profile = {
            "user_id": user_id,
            "genres": genre_counts,
            "authors": author_counts,
            "average_rating": avg_rating,
            "interaction_count": len(interactions),
        }
        self.user_profiles_cache[user_id] = profile
        return profile

    def collaborative_filtering_recommendations(self, user_id: str, limit: int) -> List[Book]:
        """Mock collaborative filtering based on user interactions."""
        # For demo, get books liked by users with similar genre preferences.
        profile = self.user_profiles_cache.get(user_id) or self.build_user_profile(user_id)
        top_genres = sorted(profile["genres"].items(), key=lambda x: x[1], reverse=True)[:3]
        genres = [g[0] for g in top_genres if g[0] is not None]
        books = []
        if genres:
            books = get_books_by_genres(genres, limit=limit)
        # Assign similarity scores randomly for demo
        for book in books:
            book.similarity_score = round(random.uniform(0.5, 1.0), 2)
        return books[:limit]

    def content_based_recommendations(self, user_id: str, limit: int) -> List[Book]:
        """Mock content-based recommendations based on authors user likes."""
        profile = self.user_profiles_cache.get(user_id) or self.build_user_profile(user_id)
        top_authors = sorted(profile["authors"].items(), key=lambda x: x[1], reverse=True)[:2]
        books = []
        for author, _ in top_authors:
            author_books = get_books_by_author(author, limit=limit)
            for book in author_books:
                book.similarity_score = round(random.uniform(0.4, 0.9), 2)
            books.extend(author_books)
            if len(books) >= limit:
                break
        return books[:limit]

    def trending_recommendations(self, limit: int) -> List[Book]:
        """Get trending books."""
        books = get_trending_books(limit=limit)
        for book in books:
            book.similarity_score = round(random.uniform(0.3, 0.8), 2)
        return books

    def generate_recommendations(self, user_id: str, limit: int = 10) -> RecommendationResult:
        """Generate combined recommendations."""
        profile = self.user_profiles_cache.get(user_id) or self.build_user_profile(user_id)
        collab_books = self.collaborative_filtering_recommendations(user_id, limit=limit//2)
        content_books = self.content_based_recommendations(user_id, limit=limit//3)
        trending_books = self.trending_recommendations(limit=limit - len(collab_books) - len(content_books))

        combined_books_dict = {}
        for book in collab_books + content_books + trending_books:
            combined_books_dict[book.id] = book  # overwrite duplicates

        combined_books = list(combined_books_dict.values())
        combined_books.sort(key=lambda b: b.similarity_score or 0, reverse=True)
        combined_books = combined_books[:limit]

        reasons = [
            f"Collaborative Filtering yielded {len(collab_books)} books",
            f"Content-Based Filtering yielded {len(content_books)} books",
            f"Trending books included {len(trending_books)} books",
            f"User interaction count: {profile['interaction_count']}",
        ]

        return RecommendationResult(
            books=combined_books,
            algorithm_used="Hybrid: Collaborative + Content-Based + Trending",
            confidence_score=0.9,
            reasons=reasons
        )

    def debug_user_profile(self, user_id: str) -> Dict[str, Any]:
        """Return detailed user profile for debugging."""
        profile = self.user_profiles_cache.get(user_id) or self.build_user_profile(user_id)
        return profile


# ------------------------------
# API Wrapper
# ------------------------------

class EnhancedRecommendationAPI:
    def __init__(self, engine: EnhancedBookRecommendationEngine):
        self.engine = engine

    def get_recommendations(self, user_id: str, limit: int = 10) -> Dict[str, Any]:
        rec_result = self.engine.generate_recommendations(user_id, limit=limit)
        return rec_result.to_dict()

    def get_user_profile_debug(self, user_id: str) -> Dict[str, Any]:
        return self.engine.debug_user_profile(user_id)


# ------------------------------
# Main test section
# ------------------------------

if __name__ == "__main__":
    engine = EnhancedBookRecommendationEngine()
    api = EnhancedRecommendationAPI(engine)

    test_user_id = "testuser123"

    print("Generating recommendations for user:", test_user_id)
    recommendations = api.get_recommendations(test_user_id, limit=10)
    for idx, book in enumerate(recommendations["books"], 1):
        print(f"{idx}. {book['title']} by {book['author']} (Genre: {book['genre']}, Score: {book.get('similarity_score', 'N/A')})")

    print("\nUser profile debug info:")
    profile = api.get_user_profile_debug(test_user_id)
    print(profile)
