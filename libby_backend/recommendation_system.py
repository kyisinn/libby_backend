# libby_backend/recommendation_system.py
import random
from datetime import datetime
from typing import List, Optional

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


# ------------------------------
# Engine
# ------------------------------

class EnhancedBookRecommendationEngine:
    def __init__(self):
        self.interactions = {}  # user_id -> interaction count
        self.user_profiles = {}  # user_id -> genres

    def record_user_interaction(self, user_id: str, book_id: int,
                                interaction_type: str, weight: float = 1.0,
                                rating: Optional[float] = None):
        """Record a fake user interaction for prototyping"""
        self.interactions[user_id] = self.interactions.get(user_id, 0) + 1

    def get_user_profile_enhanced(self, user_id: str):
        """Return a mock profile with genres, authors, etc."""
        genres = ["Technology & Engineering", "Computers", "Business & Economics"]
        profile = type("Profile", (), {})()
        profile.user_id = user_id
        profile.email = f"{user_id}@example.com"
        profile.selected_genres = genres
        profile.favorite_authors = ["John Smith", "Jane Doe"]
        profile.preferred_rating_threshold = 3.5
        profile.reading_history = []
        profile.wishlist = []
        profile.interaction_weights = {"click": 1.2, "wishlist_add": 3.0}
        return profile

    def get_recommendations_for_user_enhanced(self, user_id: str, email: str = None,
                                              selected_genres: List[str] = None,
                                              limit: int = 10,
                                              force_refresh: bool = False) -> RecommendationResult:
        """Return mock recommendations"""
        books = []
        genres = selected_genres or ["Technology & Engineering", "Computers", "Business & Economics"]
        for i in range(limit):
            books.append(Book(
                id=random.randint(1000, 9999),
                title=f"Book Title {i+1}",
                author=f"Author {i+1}",
                genre=random.choice(genres),
                cover_image_url=None,
                rating=round(random.uniform(3.0, 5.0), 1),
                similarity_score=round(random.uniform(0.3, 1.0), 2)
            ))
        return RecommendationResult(
            books=books,
            algorithm_used="Database Hybrid (Collaborative + Content + Trending)",
            confidence_score=0.85,
            reasons=[f"Interactions: {self.interactions.get(user_id, 0)}",
                     f"Top genres: {', '.join(genres)}"]
        )

    def get_quality_trending_books(self, limit: int, exclude_ids=None, profile=None):
        """Return mock trending books"""
        return [Book(
            id=random.randint(1000, 9999),
            title=f"Trending Book {i+1}",
            author=f"Famous Author {i+1}",
            genre="General",
            rating=round(random.uniform(3.0, 5.0), 1)
        ) for i in range(limit)]

    def get_diversity_books(self, profile, limit: int, exclude_ids=None):
        """Return books from random unexplored genres"""
        random_genres = ["History", "Philosophy", "Art", "Science Fiction"]
        return [Book(
            id=random.randint(1000, 9999),
            title=f"Diversity Book {i+1}",
            author=f"Author {i+1}",
            genre=random.choice(random_genres),
            rating=round(random.uniform(2.5, 5.0), 1)
        ) for i in range(limit)]

    def get_books_by_author(self, author_name: str, limit: int, exclude_ids=None):
        return [Book(
            id=random.randint(1000, 9999),
            title=f"{author_name}'s Book {i+1}",
            author=author_name,
            genre="Author Special",
            rating=round(random.uniform(3.0, 5.0), 1)
        ) for i in range(limit)]


# ------------------------------
# API Wrapper
# ------------------------------

class EnhancedRecommendationAPI:
    def __init__(self, engine: EnhancedBookRecommendationEngine):
        self.engine = engine

    def refresh_recommendations(self, user_id: str, limit: int = 10):
        return self.engine.get_recommendations_for_user_enhanced(user_id, limit=limit).__dict__

    def record_interaction(self, user_id: str, book_id: int, interaction_type: str):
        self.engine.record_user_interaction(user_id, book_id, interaction_type)

    def get_user_stats(self, user_id: str):
        return {
            "interactions": self.engine.interactions.get(user_id, 0),
            "top_genres": ["Technology & Engineering", "Computers", "Business & Economics"]
        }


# ------------------------------
# Utility Functions
# ------------------------------

def recommend_books_for_user(user_id: str, limit: int = 10):
    return [Book(
        id=random.randint(1000, 9999),
        title=f"Simple Book {i+1}",
        author=f"Author {i+1}",
        genre="General"
    ).to_dict() for i in range(limit)]


def get_books_by_advanced_genre_search_enhanced(genre: str, limit: int = 10):
    return [Book(
        id=random.randint(1000, 9999),
        title=f"{genre} Book {i+1}",
        author=f"Author {i+1}",
        genre=genre
    ) for i in range(limit)]


def get_quality_trending_books_fallback(limit: int = 10, exclude_ids=None):
    return [Book(
        id=random.randint(1000, 9999),
        title=f"Fallback Trending Book {i+1}",
        author=f"Famous Author {i+1}",
        genre="General"
    ) for i in range(limit)]


def get_personalized_recommendations_fixed(user_id: str, limit: int = 10):
    engine = EnhancedBookRecommendationEngine()
    return engine.get_recommendations_for_user_enhanced(user_id, limit=limit)
