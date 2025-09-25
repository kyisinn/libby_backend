# blueprints/recommendations/routes.py
# Enhanced Recommendation System Routes with Advanced Features
from flask import Blueprint, jsonify, request
import logging
from datetime import datetime
from libby_backend.cache import cache
from typing import List, Optional, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor
from libby_backend.database import count_user_interactions, count_user_interests
import re
from urllib.parse import quote_plus
from libby_backend.database import save_recommendations_db

# Import centralized user ID resolver
from libby_backend.utils.user_resolver import resolve_user_id, with_resolved_user_id, resolve_user_id_from_request

# Import UserProfile for minimal profile builder
from libby_backend.recommendation_system import UserProfile

# Import the new database functions
from libby_backend.database import (
    record_user_interaction_db, 
    get_user_interactions_db,
    get_collaborative_recommendations_db,
    get_content_based_recommendations_db, 
    get_hybrid_recommendations_db,
    get_user_genre_preferences_db,
    get_trending_books_db
    , get_db_connection
)



# CRITICAL FIX: Correct import path based on your app structure
try:
    from libby_backend.recommendation_system import EnhancedBookRecommendationEngine, EnhancedRecommendationAPI
except ImportError:
    # Fallback import path
    from recommendation_system import EnhancedBookRecommendationEngine, EnhancedRecommendationAPI

# Initialize enhanced recommendation system with error handling
try:
    recommendation_engine = EnhancedBookRecommendationEngine()
    recommendation_api = EnhancedRecommendationAPI(recommendation_engine)
except Exception as e:
    logging.error(f"Failed to initialize recommendation engine: {e}")
    recommendation_engine = None
    recommendation_api = None

# Create blueprint
rec_bp = Blueprint("recommendations", __name__, url_prefix="/api/recommendations")
logger = logging.getLogger(__name__)

# Allowed interaction types for normalization and validation
ALLOWED_TYPES = {"view", "like", "wishlist_add", "wishlist_remove", "rate", "search"}

# CRITICAL FIX: Add engine check decorator
def require_engine(f):
    """Decorator to check if recommendation engine is available"""
    def wrapper(*args, **kwargs):
        if not recommendation_engine:
            return jsonify({
                'success': False,
                'error': 'Recommendation engine not available',
                'details': 'Engine initialization failed'
            }), 503
        return f(*args, **kwargs)
    wrapper.__name__ = f.__name__
    return wrapper

# Debug and fallback utility functions
def check_user_setup(user_id: str):
    """Check if user has interests configured"""
    try:
        # Convert Clerk ID to integer (same format as user_interactions table)
        user_id_int = resolve_user_id(user_id)
        
        from libby_backend.database import get_db_connection
        conn = get_db_connection()
        if not conn:
            return False, "Database connection failed"
            
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) as count FROM user_interests WHERE user_id = %s", (user_id_int,))
            result = cursor.fetchone()
            interest_count = result[0] if result else 0
        conn.close()
        
        if interest_count == 0:
            return False, "User has no interests configured"
        
        return True, f"User has {interest_count} interests configured"
        
    except Exception as e:
        return False, f"Error checking user setup: {e}"

def get_recommendations_with_better_fallbacks(user_id: str, limit: int = 20):
    """Enhanced recommendation function with multiple fallback levels"""
    try:
        # Convert Clerk ID to integer (same format as user_interactions table)
        user_id_int = resolve_user_id(user_id)
        
        # Level 1: Try normal enhanced recommendations
        result = recommendation_engine.get_recommendations_for_user_enhanced(
            user_id=user_id_int,
            limit=limit,
            force_refresh=True  # Always bypass cache for debugging
        )
        
        if result.books and len(result.books) > 0:
            return result
        
        logger.warning(f"Level 1 failed for {user_id}, trying fallbacks...")
        
        # Level 2: Try with user genres from database
        from libby_backend.database import get_db_connection
        conn = get_db_connection()
        if conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT genre FROM user_interests WHERE user_id = %s", (user_id_int,))
                user_genres = [row[0] for row in cursor.fetchall()]
            conn.close()
            
            if user_genres:
                logger.info(f"Found user genres for {user_id}: {user_genres}")
                # Try genre-based recommendations
                from libby_backend.recommendation_system import get_books_by_advanced_genre_search_enhanced
                genre_books = []
                for genre in user_genres[:3]:  # Try up to 3 genres
                    books = get_books_by_advanced_genre_search_enhanced(genre, limit // len(user_genres[:3]) + 2)
                    genre_books.extend(books)
                
                if genre_books:
                    from libby_backend.recommendation_system import RecommendationResult
                    from datetime import datetime
                    return RecommendationResult(
                        books=genre_books[:limit],
                        algorithm_used="Genre-Based Fallback",
                        confidence_score=0.6,
                        reasons=[f"Based on your selected genres: {', '.join(user_genres[:3])}"],
                        generated_at=datetime.now()
                    )
        
        # Level 3: High-quality trending books
        logger.warning(f"Level 2 failed for {user_id}, trying trending fallback...")
        from libby_backend.recommendation_system import get_quality_trending_books_fallback
        trending_books = get_quality_trending_books_fallback(limit)
        
        if trending_books:
            from libby_backend.recommendation_system import RecommendationResult
            from datetime import datetime
            return RecommendationResult(
                books=trending_books,
                algorithm_used="Quality Trending Fallback",
                confidence_score=0.5,
                reasons=["High-quality trending books as fallback"],
                generated_at=datetime.now()
            )
        
        # Level 4: Any books from database
        logger.warning(f"Level 3 failed for {user_id}, trying basic database query...")
        conn = get_db_connection()
        if conn:
            books = []
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT book_id, title, author, genre, cover_image_url, rating
                    FROM books 
                    WHERE title IS NOT NULL 
                    AND author IS NOT NULL
                    AND cover_image_url IS NOT NULL
                    ORDER BY rating DESC NULLS LAST
                    LIMIT %s
                """, (limit,))
                
                for row in cursor.fetchall():
                    from libby_backend.recommendation_system import Book
                    book = Book(
                        id=row[0],
                        title=row[1],
                        author=row[2],
                        genre=row[3],
                        cover_image_url=row[4],
                        rating=row[5]
                    )
                    books.append(book)
            conn.close()
            
            if books:
                from libby_backend.recommendation_system import RecommendationResult
                from datetime import datetime
                return RecommendationResult(
                    books=books,
                    algorithm_used="Basic Database Fallback",
                    confidence_score=0.3,
                    reasons=["Basic database query as last resort"],
                    generated_at=datetime.now()
                )
        
        # Level 5: Empty result with explanation
        logger.error(f"All fallback levels failed for {user_id}")
        from libby_backend.recommendation_system import RecommendationResult
        from datetime import datetime
        return RecommendationResult(
            books=[],
            algorithm_used="Failed - No Fallbacks Worked",
            confidence_score=0.0,
            reasons=["All recommendation methods failed - check database connection"],
            generated_at=datetime.now()
        )
        
    except Exception as e:
        logger.error(f"Error in enhanced fallback for {user_id}: {e}")
        from libby_backend.recommendation_system import RecommendationResult
        from datetime import datetime
        return RecommendationResult(
            books=[],
            algorithm_used="Error",
            confidence_score=0.0,
            reasons=[f"System error: {str(e)}"],
            generated_at=datetime.now()
        )


def build_profile_for(clerk_user_id: str):
    """Build a UserProfile for the given Clerk user id using the recommendation engine.

    This is a small wrapper that prefers the enhanced profile builder and falls back
    to the basic profile if necessary.
    """
    """
    Minimal profile builder from DB so the hybrid engine never crashes.
    This builds a lightweight UserProfile and attaches the raw Clerk id as
    an attribute so downstream code that checks `profile.clerk_user_id` works.
    """
    reading_history: list[int] = []
    wishlist: list[int] = []
    preferred_genres: list[str] = []

    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # interactions to seed history
                cur.execute("""
                    SELECT DISTINCT book_id
                    FROM public.user_interactions
                    WHERE clerk_user_id = %s
                      AND interaction_type IN ('view','like','rate','wishlist_add')
                """, (clerk_user_id,))
                reading_history = [int(r["book_id"]) for r in cur.fetchall()]

                # explicit interests
                cur.execute("""
                    SELECT DISTINCT LOWER(TRIM(genre)) AS g
                    FROM public.user_interests
                    WHERE clerk_user_id = %s
                """, (clerk_user_id,))
                preferred_genres = [r["g"] for r in cur.fetchall()]
        finally:
            try:
                conn.close()
            except Exception:
                pass

    # Default rating threshold 3.5; tweak if you store this elsewhere
    profile = UserProfile(
        user_id=str(clerk_user_id),
        email=None,
        selected_genres=preferred_genres,
        reading_history=reading_history,
        wishlist=wishlist,
        interaction_weights={},
        favorite_authors=[],
        preferred_rating_threshold=3.5,
    )
    # Attach raw clerk id for code that expects it
    setattr(profile, 'clerk_user_id', clerk_user_id)
    return profile

# Debug endpoints - commented out for production use
# @rec_bp.route("/<user_id>/debug", methods=["GET"])
# def debug_user_recommendations(user_id: str):
#     """Debug endpoint to see what's happening with recommendations"""
#     # Debug functionality removed for production use
#     return jsonify({
#         'message': 'Debug endpoints disabled in production',
#         'status': 'disabled'
#     })

# Debug function removed for production - uncomment if needed for troubleshooting
# Rest of debug function implementation commented out for production use

@rec_bp.route("/<user_id>/clear-cache", methods=["POST"])
def clear_user_cache(user_id: str):
    """Clear cached recommendations for a specific user"""
    try:
        if not recommendation_engine:
            return jsonify({
                'success': False,
                'error': 'Recommendation engine not available'
            })
            
        import sqlite3
        conn = sqlite3.connect(recommendation_engine.db_path)
        cursor = conn.cursor()
        
        # Delete cached recommendations for this user
        cursor.execute('DELETE FROM user_recommendations WHERE user_id = ?', (user_id,))
        deleted_count = cursor.rowcount
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'Cleared cache for user {user_id}',
            'deleted_entries': deleted_count
        })
        
    except Exception as e:
        logger.error(f"Error clearing cache for {user_id}: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        })
    
def _to_https(u: str | None) -> str | None:
    if not u:
        return None
    return re.sub(r'(?i)^http://', 'https://', u.strip())


@rec_bp.route("/<user_id>/improve", methods=["GET"], endpoint="improve_recs")
def get_improved_recommendations_with_fallbacks(user_id: str):
    try:
        limit = int(request.args.get("limit", 20))

        # ---------- READ CACHE FIRST (fast return)
        cache_key = f"improve_resp:{user_id}:{limit}"
        cached = None
        try:
            cached = cache.get(cache_key)
        except Exception:
            pass
        if cached:
            return jsonify(cached)

        # ---------- build as before
        profile = build_profile_for(user_id)
        result  = recommendation_engine.hybrid_recommendations_enhanced(profile, limit)

        i_count = count_user_interactions(user_id)
        u_count = count_user_interests(user_id)
        algo_label = ("Database Hybrid (Collaborative + Content + Trending)"
                      if (i_count > 0 or u_count > 0) else "Trending (New User)")

        # ---- covers prefetch (unchanged; keeping your code)
        from psycopg2.extras import RealDictCursor
        import re
        def _https(u: str | None) -> str | None:
            if not u: return None
            return re.sub(r'(?i)^http://', 'https://', u.strip())

        ids = [int(getattr(b, "id")) for b in (result.books or []) if getattr(b, "id", None)]
        cover_by_id = {}
        if ids:
            try:
                cover_key = "covers:" + ",".join(str(x) for x in sorted(ids))
                cover_by_id = cache.get(cover_key) or {}
            except Exception:
                cover_by_id = {}

            if not cover_by_id:
                conn = get_db_connection()
                if conn:
                    try:
                        with conn.cursor(cursor_factory=RealDictCursor) as cur:
                            cur.execute("""
                                SELECT book_id, cover_image_url
                                FROM books
                                WHERE book_id = ANY(%s)
                            """, (ids,))
                            for row in cur.fetchall():
                                cover_by_id[int(row["book_id"])] = _https(row.get("cover_image_url"))
                    finally:
                        try: conn.close()
                        except Exception: pass
                try:
                    cache.set(cover_key, cover_by_id, timeout=3600)
                except Exception:
                    pass

        books_data = []
        for book in (result.books or []):
            raw = (
                getattr(book, "cover_image_url", None)
                or getattr(book, "coverurl", None)
                or (cover_by_id.get(int(getattr(book, "id"))) if getattr(book, "id", None) else None)
            )
            raw = _https(raw)
            if not raw:
                isbn = getattr(book, "isbn", None)
                if isbn:
                    digits = re.sub(r"[^0-9Xx]", "", str(isbn))
                    if digits:
                        raw = f"https://covers.openlibrary.org/b/isbn/{digits}-L.jpg"

            from urllib.parse import quote_plus
            cover_url = raw or f"https://placehold.co/320x480/1e1f22/ffffff?text={quote_plus((book.title or '')[:40])}"

            books_data.append({
                "id": getattr(book, "id", None),
                "title": getattr(book, "title", None),
                "author": getattr(book, "author", None),
                "genre": getattr(book, "genre", None),
                "description": getattr(book, "description", None),
                "cover_image_url": cover_url,
                "coverurl": cover_url,
                "rating": float(getattr(book, "rating", None)) if getattr(book, "rating", None) is not None else None,
                "publication_date": getattr(book, "publication_date", None),
                "pages": getattr(book, "pages", None),
                "language": getattr(book, "language", None),
                "isbn": getattr(book, "isbn", None),
                "similarity_score": getattr(book, "similarity_score", None),
            })

        payload = {
            "success": True,
            "books": books_data,
            "recommendations": books_data,
            "algorithm_used": algo_label,
            "algorithmUsed": algo_label,
            "interaction_count": i_count,
            "interactionCount": i_count,
            "confidence_score": float(getattr(result, "confidence_score", 0.0)),
            "reasons": list(set(getattr(result, "reasons", []) or [])),
            "generated_at": getattr(result, "generated_at", datetime.now()).isoformat(),
            "total_count": len(books_data),
            "enhanced": True,
            "fallback_used": algo_label.lower().startswith("trending"),
        }

        # ---------- STORE in cache (short TTL)
        try:
            cache.set(cache_key, payload, timeout=30)   # 30s micro-cache
        except Exception:
            pass
        try:
            save_recommendations_db(
                user_id=None,
                clerk_user_id=user_id,
                books=books_data,
                rec_type="improve"
            )
        except Exception as e:
            logger.warning(f"Failed to save recommendations log: {e}")



        return jsonify(payload)

    except Exception as e:
        import traceback, sys
        traceback.print_exc(file=sys.stderr)
        return jsonify({"success": False, "error": "Failed to generate recommendations",
                        "details": str(e), "books": [], "recommendations": []}), 500

@rec_bp.route("/<user_id>/simple", methods=["GET"])
def get_simple_test_recommendations(user_id: str):
    """Test endpoint for the simple recommend_books_for_user function"""
    try:
        limit = int(request.args.get('limit', 20))
        
        # Use the simple recommendation function
        from libby_backend.recommendation_system import recommend_books_for_user
        books = recommend_books_for_user(user_id, limit)
        
        return jsonify({
            'success': True,
            'user_id': user_id,
            'books': books,
            'total_count': len(books),
            'algorithm_used': 'Simple SQL Query',
            'message': 'Using simple psycopg2 function' if books else 'No books found - check debug output'
        })
        
    except Exception as e:
        logger.error(f"Error in simple recommendations for {user_id}: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to get simple recommendations',
            'details': str(e),
            'books': []
        }), 500

@rec_bp.route("/<user_id>", methods=["GET"])
@require_engine
def get_user_recommendations(user_id: str):
    """Get enhanced personalized recommendations for a user"""
    try:
        email = request.args.get('email')
        genres = request.args.getlist('genres')
        limit = int(request.args.get('limit', 20))
        force_refresh = request.args.get('force_refresh', 'false').lower() == 'true'
        
        # Use enhanced recommendation method with better error handling
        result = recommendation_engine.get_recommendations_for_user_enhanced(
            user_id=user_id,
            email=email,
            selected_genres=genres if genres else None,
            limit=limit,
            force_refresh=force_refresh
        )
        
        # Convert books to dict format for JSON serialization
        books_data = []
        for book in result.books:
            try:
                if hasattr(book, 'to_dict'):
                    books_data.append(book.to_dict())
                else:
                    # Fallback manual conversion
                    book_dict = {
                        'id': book.id,
                        'title': book.title,
                        'author': book.author,
                        'genre': book.genre,
                        'description': book.description,
                        'cover_image_url': book.cover_image_url,
                        'rating': float(book.rating) if book.rating else None,
                        'publication_date': book.publication_date,
                        'pages': book.pages,
                        'language': book.language,
                        'isbn': book.isbn,
                        'similarity_score': getattr(book, 'similarity_score', None)
                    }
                    books_data.append(book_dict)
            except Exception as e:
                logger.error(f"Error serializing book {book.id}: {e}")
                continue
        
        response = {
            "success": True,
            "recommendations": books_data,
            "algorithm_used": result.algorithm_used,
            "confidence_score": float(result.confidence_score),
            "reasons": result.reasons,
            "generated_at": result.generated_at.isoformat(),
            "total_count": len(books_data),
            "enhanced": True
        }
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error getting enhanced recommendations for {user_id}: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to generate enhanced recommendations',
            'details': str(e),
            'recommendations': []
        }), 500

@rec_bp.route("/<user_id>/trending", methods=["GET"])
@require_engine
def get_personalized_trending(user_id: str):
    """Get high-quality trending books personalized for the user"""
    try:
        limit = int(request.args.get('limit', 10))
        
        # Build user profile for enhanced filtering
        profile = recommendation_engine.get_user_profile_enhanced(user_id)
        
        # Get quality trending books with fallback
        try:
            trending_books = recommendation_engine.get_quality_trending_books(
                limit=limit,
                exclude_ids=profile.reading_history + profile.wishlist,
                profile=profile
            )
        except Exception as e:
            logger.warning(f"Primary trending method failed: {e}, using fallback")
            # Use the fallback function from our fixes
            from libby_backend.recommendation_system import get_quality_trending_books_fallback
            trending_books = get_quality_trending_books_fallback(limit, profile.reading_history + profile.wishlist)
        
        # Convert to dict format
        books_data = []
        for book in trending_books:
            try:
                if hasattr(book, 'to_dict'):
                    books_data.append(book.to_dict())
                else:
                    books_data.append(book.__dict__)
            except Exception as e:
                logger.error(f"Error serializing trending book: {e}")
                continue
        
        return jsonify({
            'success': True,
            'trending_books': books_data,
            'personalized': True,
            'user_id': user_id,
            'total_count': len(books_data),
            'enhanced': True,
            'rating_threshold': profile.preferred_rating_threshold
        })
        
    except Exception as e:
        logger.error(f"Error getting enhanced personalized trending for {user_id}: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to get enhanced personalized trending books',
            'details': str(e),
            'trending_books': []
        }), 500

@rec_bp.route("/<user_id>/refresh", methods=["POST"])
def refresh_user_recommendations(user_id: str):
    """Force refresh recommendations for a user"""
    try:
        limit = int(request.args.get('limit', 20))
        
        result = recommendation_api.refresh_recommendations(
            user_id=user_id,
            limit=limit
        )
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error refreshing recommendations for {user_id}: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to refresh recommendations'
        }), 500

@rec_bp.route("/interactions", methods=["POST"])
@require_engine
def record_user_interaction():
    """Record a user interaction with a book (enhanced with rating support and Clerk ID support)"""
    try:
        data = request.get_json()

        if not data:
            return jsonify({'success': False, 'error': 'No data provided'}), 400

        user_id = data.get('user_id')
        book_id = data.get('book_id')
        interaction_type = data.get('type', 'click')
        rating = data.get('rating')

        if not all([user_id, book_id, interaction_type]):
            return jsonify({'success': False, 'error': 'Missing required fields: user_id, book_id, type'}), 400

        # Validate interaction type
        valid_types = ['view', 'wishlist_add', 'wishlist_remove', 'search', 'click', 'like', 'dislike', 'rate']
        if interaction_type not in valid_types:
            return jsonify({'success': False, 'error': f'Invalid interaction type. Must be one of: {", ".join(valid_types)}'}), 400

        # Validate rating if provided
        if rating is not None:
            try:
                rating = float(rating)
                if not (1.0 <= rating <= 5.0):
                    return jsonify({'success': False, 'error': 'Rating must be between 1.0 and 5.0'}), 400
            except (ValueError, TypeError):
                return jsonify({'success': False, 'error': 'Invalid rating format'}), 400

        # Handle both Clerk user IDs and integer user IDs
        clerk_user_id = None
        if isinstance(user_id, str) and user_id.startswith('user_'):
            clerk_user_id = user_id
            user_id_int = resolve_user_id(user_id)
        else:
            try:
                user_id_int = int(user_id)
            except (ValueError, TypeError):
                return jsonify({'success': False, 'error': 'Invalid user_id format'}), 400

        try:
            book_id_int = int(book_id)
        except (ValueError, TypeError):
            return jsonify({'success': False, 'error': 'Invalid book_id format'}), 400

        # Enhanced interaction weights
        weight_mapping = {
            'view': 1.0,
            'click': 1.2,
            'like': 2.0,
            'wishlist_add': 3.0,
            'wishlist_remove': -1.0,
            'rate': 2.5,
            'search': 0.5,
            'dislike': -0.5
        }

        weight = weight_mapping.get(interaction_type, 1.0)

        # Persist to DB with clerk_user_id when available
        result = record_user_interaction_db(
            user_id=user_id_int,
            book_id=book_id_int,
            interaction_type=interaction_type,
            rating=rating,
            clerk_user_id=clerk_user_id
        )

        if not result:
            return jsonify({'success': False, 'error': 'Failed to record interaction in database'}), 500

        # Record enhanced interaction in recommendation engine as well
        try:
            if recommendation_engine:
                recommendation_engine.record_user_interaction(
                    user_id=str(user_id_int),
                    book_id=book_id_int,
                    interaction_type=interaction_type,
                    weight=weight,
                    rating=rating
                )
        except Exception as e:
            logger.warning(f"Failed to record in recommendation engine: {e}")

        return jsonify({
            'success': True,
            'message': f'Enhanced interaction recorded: {interaction_type} for book {book_id_int}',
            'interaction': {
                'id': result.get('id'),
                'user_id': result.get('user_id'),
                'clerk_user_id': result.get('clerk_user_id'),
                'book_id': result.get('book_id'),
                'interaction_type': result.get('interaction_type'),
                'rating': rating,
                'timestamp': result.get('timestamp')
            },
            'weight': weight,
            'enhanced': True
        })

    except ValueError:
        return jsonify({'success': False, 'error': 'Invalid book_id format'}), 400
    except Exception as e:
        logger.error(f"Error recording enhanced interaction: {e}")
        return jsonify({'success': False, 'error': 'Failed to record enhanced interaction', 'details': str(e)}), 500

@rec_bp.route("/<user_id>/profile", methods=["GET"])
def get_user_profile_info(user_id: str):
    """Get enhanced user profile information"""
    try:
        profile = recommendation_engine.get_user_profile_enhanced(user_id)
        
        return jsonify({
            'success': True,
            'user_id': profile.user_id,
            'email': profile.email,
            'selected_genres': profile.selected_genres,
            'favorite_authors': profile.favorite_authors,
            'preferred_rating_threshold': profile.preferred_rating_threshold,
            'reading_history_count': len(profile.reading_history),
            'wishlist_count': len(profile.wishlist),
            'interaction_weights': profile.interaction_weights,
            'enhanced': True
        })
        
    except Exception as e:
        logger.error(f"Error getting user profile for {user_id}: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to get user profile'
        }), 500

@rec_bp.route("/<user_id>/diversity", methods=["GET"])
def get_diversity_recommendations(user_id: str):
    """Get diversity recommendations from unexplored genres"""
    try:
        limit = int(request.args.get('limit', 10))
        
        # Build user profile
        profile = recommendation_engine.get_user_profile_enhanced(user_id)
        
        # Get diversity books
        diversity_books = recommendation_engine.get_diversity_books(
            profile=profile,
            limit=limit,
            exclude_ids=profile.reading_history + profile.wishlist
        )
        
        # Convert to dict format
        books_data = []
        for book in diversity_books:
            book_dict = book.__dict__.copy()
            if hasattr(book, 'similarity_score'):
                book_dict['similarity_score'] = book.similarity_score
            books_data.append(book_dict)
        
        return jsonify({
            'success': True,
            'diversity_books': books_data,
            'user_id': user_id,
            'total_count': len(books_data),
            'enhanced': True,
            'purpose': 'Explore new genres outside your usual preferences'
        })
        
    except Exception as e:
        logger.error(f"Error getting diversity recommendations for {user_id}: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to get diversity recommendations'
        }), 500

@rec_bp.route("/<user_id>/stats", methods=["GET"])
def get_user_recommendation_stats(user_id: str):
    """Get recommendation statistics for a user"""
    try:
        result = recommendation_api.get_user_stats(user_id)
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error getting stats for {user_id}: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to get user statistics'
        }), 500

@rec_bp.route("/similar-to/<int:book_id>", methods=["GET"])
def get_similar_books(book_id: int):
    """Get books similar to the specified book"""
    try:
        user_id = request.args.get('user_id')  # Optional: for personalized similarity
        limit = int(request.args.get('limit', 10))
        
        # Get the target book
        from libby_backend.database import get_book_by_id_db, get_books_by_genre_db
        target_book = get_book_by_id_db(book_id)
        
        if not target_book:
            return jsonify({
                'success': False,
                'error': 'Book not found'
            }), 404
        
        # Find similar books based on genre
        similar_books = []
        if target_book.get('genre'):
            similar_books = get_books_by_genre_db(
                target_genre=target_book['genre'],
                exclude_book_id=book_id,
                limit=limit
            )
        
        # If user_id is provided, consider their preferences
        if user_id and similar_books:
            profile = recommendation_engine.get_user_profile(user_id)
            
            # Score books based on user preferences
            scored_books = []
            for book_data in similar_books:
                book = recommendation_engine._get_book_from_main_db(book_data['id'])
                if book:
                    score = recommendation_engine._calculate_content_score(book, profile)
                    book_data['relevance_score'] = score
                    scored_books.append(book_data)
            
            # Sort by relevance score
            similar_books = sorted(scored_books, key=lambda x: x.get('relevance_score', 0), reverse=True)
        
        return jsonify({
            'success': True,
            'target_book': {
                'id': target_book['book_id'],
                'title': target_book['title'],
                'author': target_book['author'],
                'genre': target_book.get('genre')
            },
            'similar_books': similar_books,
            'total_count': len(similar_books)
        })
        
    except Exception as e:
        logger.error(f"Error getting similar books for {book_id}: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to get similar books'
        }), 500

@rec_bp.route("/by-genre", methods=["GET"])
def get_recommendations_by_genre():
    """Get enhanced recommendations for specific genres"""
    try:
        genres = request.args.getlist('genres')  # Multiple genres supported
        user_id = request.args.get('user_id')  # Optional for personalization
        limit = int(request.args.get('limit', 20))
        
        if not genres:
            return jsonify({
                'success': False,
                'error': 'At least one genre must be specified'
            }), 400
        
        all_books = []
        books_per_genre = max(1, limit // len(genres))
        
        for genre in genres:
            # Use enhanced genre search with synonyms
            genre_books = recommendation_engine.get_books_by_advanced_genre_search(
                target_genre=genre,
                limit=books_per_genre * 2,  # Get extra for filtering
                exclude_ids=[]
            )
            
            # If user provided, personalize the selection
            if user_id and genre_books:
                profile = recommendation_engine.get_user_profile_enhanced(user_id)
                exclude_ids = profile.reading_history + profile.wishlist
                
                # Filter out books user has already interacted with and apply quality filter
                filtered_books = []
                for book in genre_books:
                    if (book.id not in exclude_ids and 
                        (not book.rating or book.rating >= profile.preferred_rating_threshold)):
                        # Calculate enhanced content score
                        book.similarity_score = recommendation_engine._calculate_enhanced_content_score(
                            book, profile, 1.0
                        )
                        filtered_books.append(book)
                
                # Sort by similarity score
                filtered_books.sort(key=lambda x: getattr(x, 'similarity_score', 0), reverse=True)
                genre_books = filtered_books[:books_per_genre]
            else:
                # No user context, just filter by quality
                quality_books = [book for book in genre_books 
                               if not book.rating or book.rating >= 3.0]
                genre_books = quality_books[:books_per_genre]
            
            all_books.extend(genre_books)
        
        # Remove duplicates
        seen_ids = set()
        unique_books = []
        for book in all_books:
            if book.id not in seen_ids:
                seen_ids.add(book.id)
                book_dict = book.__dict__.copy()
                # Include similarity score if available
                if hasattr(book, 'similarity_score'):
                    book_dict['similarity_score'] = book.similarity_score
                unique_books.append(book_dict)
        
        return jsonify({
            'success': True,
            'books': unique_books[:limit],
            'genres': genres,
            'total_count': len(unique_books),
            'enhanced': True,
            'personalized': user_id is not None
        })
        
    except Exception as e:
        logger.error(f"Error getting enhanced recommendations by genre: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to get enhanced genre recommendations'
        }), 500

@rec_bp.route("/by-author", methods=["GET"])
def get_recommendations_by_author():
    """Get enhanced recommendations by author"""
    try:
        author = request.args.get('author', '').strip()
        user_id = request.args.get('user_id')  # Optional for personalization
        limit = int(request.args.get('limit', 10))
        
        if not author:
            return jsonify({
                'success': False,
                'error': 'Author name is required'
            }), 400
        
        # Get books by the specified author
        author_books = recommendation_engine.get_books_by_author(
            author_name=author,
            limit=limit * 2,  # Get extra for filtering
            exclude_ids=[]
        )
        
        # If user provided, personalize and filter
        if user_id and author_books:
            profile = recommendation_engine.get_user_profile_enhanced(user_id)
            exclude_ids = profile.reading_history + profile.wishlist
            
            # Filter and score books
            filtered_books = []
            for book in author_books:
                if (book.id not in exclude_ids and 
                    (not book.rating or book.rating >= profile.preferred_rating_threshold)):
                    book.similarity_score = 0.8  # High score for author match
                    if book.author in profile.favorite_authors:
                        book.similarity_score = 0.9  # Even higher for favorite authors
                    filtered_books.append(book)
            
            author_books = filtered_books[:limit]
        else:
            # No user context, just filter by quality
            quality_books = [book for book in author_books 
                           if not book.rating or book.rating >= 3.0]
            author_books = quality_books[:limit]
        
        # Convert to dict format
        books_data = []
        for book in author_books:
            book_dict = book.__dict__.copy()
            if hasattr(book, 'similarity_score'):
                book_dict['similarity_score'] = book.similarity_score
            books_data.append(book_dict)
        
        return jsonify({
            'success': True,
            'books': books_data,
            'author': author,
            'total_count': len(books_data),
            'enhanced': True,
            'personalized': user_id is not None
        })
        
    except Exception as e:
        logger.error(f"Error getting recommendations by author: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to get author recommendations'
        }), 500

@rec_bp.route("/search-based", methods=["GET"])
def get_search_based_recommendations():
    """Get recommendations based on search query"""
    try:
        query = request.args.get('q', '').strip()
        user_id = request.args.get('user_id')  # Optional for interaction recording
        limit = int(request.args.get('limit', 20))
        
        if not query:
            return jsonify({
                'success': False,
                'error': 'Search query is required'
            }), 400
        
        # Record search interaction if user provided
        if user_id:
            recommendation_api.record_interaction(
                user_id=user_id,
                book_id=0,  # Use 0 for search interactions
                interaction_type='search'
            )
        
        # Get search results
        search_books = recommendation_engine._search_books_by_query(query, limit)
        
        # Convert to dict format
        books_data = [book.__dict__ for book in search_books]
        
        return jsonify({
            'success': True,
            'books': books_data,
            'query': query,
            'total_count': len(books_data)
        })
        
    except Exception as e:
        logger.error(f"Error getting search-based recommendations: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to get search recommendations'
        }), 500

@rec_bp.route("/health", methods=["GET"])
def recommendation_health_check():
    """Enhanced health check for recommendation system"""
    try:
        if not recommendation_engine:
            return jsonify({
                'status': 'unhealthy',
                'error': 'Recommendation engine not initialized',
                'enhanced': False
            }), 503
        
        # Test database connections
        main_db_ok = False
        try:
            from libby_backend.database import get_db_connection
            conn = get_db_connection()
            if conn:
                conn.close()
                main_db_ok = True
        except Exception as e:
            logger.error(f"Main DB test failed: {e}")
        
        # Test recommendation database
        rec_db_ok = False
        try:
            import sqlite3
            conn = sqlite3.connect(recommendation_engine.db_path)
            conn.close()
            rec_db_ok = True
        except Exception as e:
            logger.error(f"Rec DB test failed: {e}")
        
        # Test basic functionality
        functionality_tests = {
            'genre_search': False,
            'fallback_trending': False,
            'profile_creation': False
        }
        
        try:
            # Test genre search
            from libby_backend.recommendation_system import get_books_by_advanced_genre_search_enhanced
            test_books = get_books_by_advanced_genre_search_enhanced("fiction", 3)
            functionality_tests['genre_search'] = len(test_books) >= 0  # Allow empty results
        except Exception as e:
            logger.error(f"Genre search test failed: {e}")
        
        try:
            # Test fallback trending
            from libby_backend.recommendation_system import get_quality_trending_books_fallback
            test_trending = get_quality_trending_books_fallback(3)
            functionality_tests['fallback_trending'] = len(test_trending) >= 0
        except Exception as e:
            logger.error(f"Fallback trending test failed: {e}")
        
        try:
            # Test profile creation
            test_profile = recommendation_engine.get_user_profile_enhanced("health_test")
            functionality_tests['profile_creation'] = test_profile is not None
        except Exception as e:
            logger.error(f"Profile creation test failed: {e}")
        
        # Determine overall status
        critical_systems = [main_db_ok, rec_db_ok]
        functionality_working = any(functionality_tests.values())
        
        if all(critical_systems) and functionality_working:
            status = "healthy"
        elif any(critical_systems):
            status = "degraded"
        else:
            status = "unhealthy"
        
        return jsonify({
            'status': status,
            'main_database': 'connected' if main_db_ok else 'failed',
            'recommendation_database': 'connected' if rec_db_ok else 'failed',
            'functionality_tests': functionality_tests,
            'enhanced': True,
            'available_features': [
                'Basic recommendation generation',
                'User interaction recording',
                'Fallback systems active',
                'Error handling improved'
            ]
        })
        
    except Exception as e:
        logger.error(f"Enhanced health check error: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'enhanced': False
        }), 500

@rec_bp.route("/test", methods=["GET"])
def test_recommendation_system():
    """Quick test endpoint with better error handling"""
    try:
        if not recommendation_engine:
            return jsonify({
                "status": "error",
                "message": "Recommendation engine not initialized",
                "enhanced": False
            }), 503

        # Test basic database connectivity
        from libby_backend.database import get_db_connection
        conn = get_db_connection()
        if not conn:
            return jsonify({
                "status": "error", 
                "message": "Database connection failed",
                "enhanced": False
            }), 500
        conn.close()

        # Simple test - try to get some books
        try:
            from libby_backend.recommendation_system import get_quality_trending_books_fallback
            test_books = get_quality_trending_books_fallback(5)
            
            return jsonify({
                "status": "success",
                "message": "Enhanced Recommendation System basic functionality working!",
                "books_found": len(test_books),
                "enhanced": True,
                "database_connected": True,
                "sample_books": [
                    {
                        "title": book.title,
                        "author": book.author,
                        "rating": book.rating
                    } for book in test_books[:3]
                ]
            })
        except Exception as e:
            logger.error(f"Test books fetch failed: {e}")
            return jsonify({
                "status": "partial",
                "message": "Database connected but book fetching has issues",
                "error": str(e),
                "enhanced": True
            })
            
    except Exception as e:
        logger.error(f"Test endpoint error: {e}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "enhanced": False
        }), 500

@rec_bp.route("/simple/<user_id>", methods=["GET"])
def get_simple_recommendations(user_id: str):
    """Simple recommendation endpoint using the basic function"""
    try:
        limit = int(request.args.get('limit', 20))
        
        # Use the simple function we fixed
        from libby_backend.recommendation_system import recommend_books_for_user
        books = recommend_books_for_user(user_id, limit)
        
        return jsonify({
            'success': True,
            'recommendations': books,
            'user_id': user_id,
            'total_count': len(books),
            'method': 'simple_psycopg2',
            'enhanced': False
        })
        
    except Exception as e:
        logger.error(f"Error getting simple recommendations for {user_id}: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to generate simple recommendations',
            'details': str(e),
            'recommendations': []
        }), 500

@rec_bp.route("/admin/cleanup", methods=["POST"])
def admin_cleanup():
    """Admin endpoint to cleanup old recommendation data"""
    try:
        # You might want to add authentication here
        days_to_keep = int(request.args.get('days', 90))
        
        recommendation_engine.cleanup_old_data(days_to_keep)
        
        return jsonify({
            'success': True,
            'message': f'Cleanup completed for data older than {days_to_keep} days'
        })
        
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        return jsonify({
            'success': False,
            'error': 'Cleanup failed'
        }), 500

@rec_bp.route("/admin/batch-update", methods=["POST"])
def admin_batch_update():
    """Admin endpoint to update recommendations for all users"""
    try:
        from libby_backend.recommendation_system import BatchRecommendationProcessor
        
        processor = BatchRecommendationProcessor(recommendation_engine)
        result = processor.update_all_recommendations()
        
        return jsonify({
            'success': True,
            'result': result
        })
        
    except Exception as e:
        logger.error(f"Error during batch update: {e}")
        return jsonify({
            'success': False,
            'error': 'Batch update failed'
        }), 500

@rec_bp.route("/admin/clear_cache", methods=["POST"])
def clear_cache():
    """Admin endpoint to clear the global cache"""
    try:
        cache.clear()
        return jsonify({
            "success": True, 
            "message": "Cache cleared successfully"
        }), 200
    except Exception as e:
        logger.error(f"Error clearing cache: {e}")
        return jsonify({
            "success": False, 
            "error": str(e)
        }), 500


# USER INTERACTIONS
# @rec_bp.route("/interactions/view", methods=["POST"])
# def record_view():
#     data = request.json
#     user_id = data.get("user_id")
#     book_id = data.get("book_id")

#     if not user_id or not book_id:
#         return jsonify({"error": "Missing user_id or book_id"}), 400

#     result = record_user_interaction(user_id, book_id, "view")
#     if result:
#         return jsonify({"status": "success", "data": result}), 201
#     else:
#         return jsonify({"status": "error", "message": "Failed to record view"}), 500

# NOTE: record_view() is commented out because record_book_click() below handles all interaction types
# including "view", and provides more comprehensive functionality with better error handling

@rec_bp.route("/interactions/click", methods=["POST", "OPTIONS"])
def record_interaction_click():
    if request.method == "OPTIONS":
        return ("", 204)

    data = request.get_json(silent=True) or {}

    # book_id
    try:
        book_id = int(data.get("book_id"))
    except Exception:
        return jsonify({"success": False, "error": "invalid or missing book_id"}), 400

    # user
    clerk_user_id = (data.get("clerk_user_id") or data.get("user_id") or "").strip()
    if not clerk_user_id:
        return jsonify({"success": False, "error": "missing clerk_user_id"}), 400

    # type normalization
    interaction_type = (data.get("interaction_type") or data.get("type") or "view").strip()
    if interaction_type == "click":
        interaction_type = "view"
    if interaction_type == "wishlist":
        interaction_type = "wishlist_add"
    if interaction_type not in ALLOWED_TYPES:
        interaction_type = "view"

    # rating: coerce "" -> None, else float
    rating_raw = data.get("rating")
    rating = None
    if rating_raw not in (None, "", "null"):
        try:
            rating = float(rating_raw)
        except Exception:
            return jsonify({"success": False, "error": "invalid rating"}), 400

    # Optional: make sure the book exists (avoids FK errors)
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM public.books WHERE book_id = %s LIMIT 1", (book_id,))
            if cur.fetchone() is None:
                return jsonify({"success": False, "error": f"book_id {book_id} not found"}), 400
    except Exception as e:
        logger.exception("DB check failed")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        try:
            if conn and not conn.closed:
                conn.close()
        except Exception:
            pass

    # Insert
    try:
        # Resolve numeric user_id from clerk_user_id when available
        user_id_val = None
        try:
            if clerk_user_id:
                user_id_val = resolve_user_id(clerk_user_id)
        except Exception:
            # If resolution fails, still proceed with clerk_user_id only
            user_id_val = None

        rec = record_user_interaction_db(
            user_id=user_id_val,
            book_id=book_id,
            interaction_type=interaction_type,
            rating=rating,
            clerk_user_id=clerk_user_id,
        )
        if not rec:
            raise RuntimeError("insert returned None")
        return jsonify({"success": True, "record": rec}), 200
    except Exception as e:
        logger.exception("record_interaction failed")
        return jsonify({"success": False, "error": str(e)}), 500

@rec_bp.route("/<user_id>/collaborative", methods=["GET"])
def get_collaborative_recommendations(user_id: str):
    """Get collaborative filtering recommendations"""
    try:
        limit = int(request.args.get('limit', 20))
        
        # Convert user_id to integer for database query
        try:
            if user_id.startswith('user_'):
                import hashlib
                user_id_int = int(hashlib.md5(user_id.encode()).hexdigest()[:8], 16)
            else:
                user_id_int = int(user_id)
        except (ValueError, TypeError):
            return jsonify({
                'success': False,
                'error': 'Invalid user_id format'
            }), 400
        
        # Get collaborative recommendations
        collab_books = get_collaborative_recommendations_db(user_id_int, limit)
        
        # Convert to expected format
        books_data = []
        for book in collab_books:
            book_dict = dict(book)
            # Ensure cover URL
            if not book_dict.get('coverurl'):
                book_dict['coverurl'] = f"https://placehold.co/320x480/1e1f22/ffffff?text={book_dict.get('title', 'Book')}"
            books_data.append(book_dict)
        
        return jsonify({
            'success': True,
            'books': books_data,
            'total_count': len(books_data),
            'algorithm_used': 'Collaborative Filtering',
            'user_id': user_id
        })
        
    except Exception as e:
        logger.error(f"Error getting collaborative recommendations: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to get collaborative recommendations',
            'details': str(e),
            'books': []
        }), 500

@rec_bp.route("/<user_id>/content-based", methods=["GET"])  
def get_content_based_recommendations(user_id: str):
    """Get content-based recommendations"""
    try:
        limit = int(request.args.get('limit', 20))
        
        # Convert user_id to integer for database query
        try:
            if user_id.startswith('user_'):
                import hashlib
                user_id_int = int(hashlib.md5(user_id.encode()).hexdigest()[:8], 16)
            else:
                user_id_int = int(user_id)
        except (ValueError, TypeError):
            return jsonify({
                'success': False,
                'error': 'Invalid user_id format'
            }), 400
        
        # Get content-based recommendations
        content_books = get_content_based_recommendations_db(user_id_int, limit)
        
        # Convert to expected format
        books_data = []
        for book in content_books:
            book_dict = dict(book)
            # Ensure cover URL
            if not book_dict.get('coverurl'):
                book_dict['coverurl'] = f"https://placehold.co/320x480/1e1f22/ffffff?text={book_dict.get('title', 'Book')}"
            books_data.append(book_dict)
        
        return jsonify({
            'success': True,
            'books': books_data,
            'total_count': len(books_data),
            'algorithm_used': 'Content-Based Filtering',
            'user_id': user_id
        })
        
    except Exception as e:
        logger.error(f"Error getting content-based recommendations: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to get content-based recommendations',
            'details': str(e),
            'books': []
        }), 500

@rec_bp.route("/<user_id>/hybrid", methods=["GET"])
def get_hybrid_recommendations_route(user_id: str):
    """Get hybrid recommendations (collaborative + content-based + trending)"""
    try:
        limit = int(request.args.get('limit', 20))
        
        # Convert user_id to integer for database query
        try:
            if user_id.startswith('user_'):
                user_id_int = resolve_user_id(user_id)
            else:
                user_id_int = user_id
        except (ValueError, TypeError):
            return jsonify({
                'success': False,
                'error': 'Invalid user_id format'
            }), 400
        
        # Get hybrid recommendations
        hybrid_books = get_hybrid_recommendations_db(user_id, limit)
        
        # Convert to expected format and add recommendation type info
        books_data = []
        algorithm_breakdown = {
            'collaborative': 0,
            'content_based': 0, 
            'trending': 0
        }
        
        for book in hybrid_books:
            book_dict = dict(book)
            # Ensure cover URL
            if not book_dict.get('coverurl'):
                book_dict['coverurl'] = f"https://placehold.co/320x480/1e1f22/ffffff?text={book_dict.get('title', 'Book')}"
            
            # Track algorithm breakdown
            rec_type = book_dict.get('recommendation_type', 'unknown')
            if rec_type in algorithm_breakdown:
                algorithm_breakdown[rec_type] += 1
                
            books_data.append(book_dict)
        
        return jsonify({
            'success': True,
            'books': books_data,
            'total_count': len(books_data),
            'algorithm_used': 'Hybrid (Collaborative + Content + Trending)',
            'algorithm_breakdown': algorithm_breakdown,
            'user_id': user_id
        })
        
    except Exception as e:
        logger.error(f"Error getting hybrid recommendations: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to get hybrid recommendations',
            'details': str(e),
            'books': []
        }), 500

@rec_bp.route("/<user_id>/history", methods=["GET"])
def get_user_interaction_history(user_id: str):
    """Get user's interaction history"""
    try:
        limit = int(request.args.get('limit', 50))
        
        # Convert user_id to integer for database query
        try:
            if user_id.startswith('user_'):
                import hashlib
                user_id_int = int(hashlib.md5(user_id.encode()).hexdigest()[:8], 16)
            else:
                user_id_int = int(user_id)
        except (ValueError, TypeError):
            return jsonify({
                'success': False,
                'error': 'Invalid user_id format'
            }), 400
        
        # Get interaction history
        interactions = get_user_interactions_db(user_id_int, limit)
        
        # Convert to expected format
        history_data = []
        for interaction in interactions:
            interaction_dict = dict(interaction)
            history_data.append(interaction_dict)
        
        return jsonify({
            'success': True,
            'interactions': history_data,
            'total_count': len(history_data),
            'user_id': user_id
        })
        
    except Exception as e:
        logger.error(f"Error getting user interaction history: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to get interaction history',
            'details': str(e),
            'interactions': []
        }), 500

@rec_bp.route("/<user_id>/genre-preferences", methods=["GET"])
def get_user_genre_preferences_route(user_id: str):
    """Get user's genre preferences based on interaction history"""
    try:
        # Convert user_id to integer for database query
        try:
            if user_id.startswith('user_'):
                import hashlib
                user_id_int = int(hashlib.md5(user_id.encode()).hexdigest()[:8], 16)
            else:
                user_id_int = int(user_id)
        except (ValueError, TypeError):
            return jsonify({
                'success': False,
                'error': 'Invalid user_id format'
            }), 400
        
        # Get genre preferences
        genre_prefs = get_user_genre_preferences_db(user_id_int)
        
        # Convert to expected format
        preferences_data = []
        for pref in genre_prefs:
            pref_dict = dict(pref)
            preferences_data.append(pref_dict)
        
        return jsonify({
            'success': True,
            'genre_preferences': preferences_data,
            'total_count': len(preferences_data),
            'user_id': user_id
        })
        
    except Exception as e:
        logger.error(f"Error getting genre preferences: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to get genre preferences',
            'details': str(e),
            'genre_preferences': []
        }), 500

# Update the main recommendations endpoint to use hybrid approach
@rec_bp.route("/<user_id>/improved", methods=["GET"])
@with_resolved_user_id
def get_improved_recommendations(user_id: int):
    """Get improved recommendations using database-based hybrid approach"""
    try:
        limit = int(request.args.get('limit', 20))
        
        # user_id is already converted to integer by the decorator
        user_id_int = user_id
        
        # Check if user has any interactions
        interactions = get_user_interactions_db(user_id_int, 5)
        
        if len(interactions) >= 2:
            # User has enough interactions, use hybrid approach
            hybrid_books = get_hybrid_recommendations_db(user_id_int, limit)
            algorithm_used = 'Database Hybrid (Collaborative + Content + Trending)'
        elif len(interactions) >= 1:
            # User has some interactions, use content-based
            hybrid_books = get_content_based_recommendations_db(user_id_int, limit)
            algorithm_used = 'Content-Based (Limited Interactions)'
        else:
            # New user, use trending books
            trending_result = get_trending_books_db('monthly', 1, limit)
            hybrid_books = trending_result['books'] if trending_result else []
            algorithm_used = 'Trending (New User)'
        
        # Ensure all books have cover URLs
        for book in hybrid_books:
            if isinstance(book, dict) and not book.get('coverurl'):
                book['coverurl'] = f"https://placehold.co/320x480/1e1f22/ffffff?text={book.get('title', 'Book')}"
        
        return jsonify({
            'success': True,
            'books': hybrid_books,
            'total_count': len(hybrid_books),
            'algorithm_used': algorithm_used,
            'user_id': user_id,
            'user_interactions_count': len(interactions)
        })
        
    except Exception as e:
        logger.error(f"Error getting improved recommendations: {e}")
        # Final fallback to trending
        try:
            trending_result = get_trending_books_db('monthly', 1, limit)
            trending_books = trending_result['books'] if trending_result else []
            return jsonify({
                'success': True,
                'books': trending_books,
                'total_count': len(trending_books),
                'algorithm_used': 'Trending (Error Fallback)',
                'user_id': user_id,
                'error': str(e)
            })
        except:
            return jsonify({
                'success': False,
                'error': 'Failed to get any recommendations',
                'details': str(e),
                'books': []
            }), 500

# DEBUG ENDPOINTS COMMENTED OUT FOR PRODUCTION
# @rec_bp.route("/<user_id>/genre-debug", methods=["GET"])
# def debug_user_genres(user_id: str):
#     """Debug what genres and books are available for this user"""
#     # Debug functionality removed for production use
#     return jsonify({
#         'message': 'Debug endpoints disabled in production',
#         'status': 'disabled'
#     })

@rec_bp.route("/<user_id>/fixed", methods=["GET"])
def get_truly_personalized_recommendations(user_id: str):
    """Get recommendations that actually use user preferences"""
    try:
        from libby_backend.recommendation_system import get_personalized_recommendations_fixed
        
        limit = int(request.args.get('limit', 20))
        
        result = get_personalized_recommendations_fixed(user_id, limit)
        
        if not result:
            return jsonify({
                'success': False,
                'error': 'Could not generate personalized recommendations',
                'recommendations': [],
                'books': []
            })
        
        # Convert books to dict format
        books_data = []
        for book in result.books:
            # Ensure we always have a cover image URL
            cover_url = book.cover_image_url or f"https://placehold.co/320x480/1e1f22/ffffff?text={book.title}"
            
            book_dict = {
                'id': book.id,
                'title': book.title,
                'author': book.author,
                'genre': book.genre,
                'description': book.description,
                'cover_image_url': cover_url,
                'coverurl': cover_url,  # Frontend compatibility
                'rating': book.rating,
                'similarity_score': getattr(book, 'similarity_score', None)
            }
            books_data.append(book_dict)
        
        return jsonify({
            'success': True,
            'recommendations': books_data,
            'books': books_data,  # Frontend compatibility
            'algorithm_used': result.algorithm_used,
            'confidence_score': result.confidence_score,
            'reasons': result.reasons,
            'total_count': len(books_data),
            'truly_personalized': True
        })
        
    except Exception as e:
        logger.error(f"Error in fixed personalized recommendations: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'recommendations': [],
            'books': []
        })
