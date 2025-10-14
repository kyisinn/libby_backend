# blueprints/recommendations/routes.py
# Simplified Recommendation System Routes - Only Essential Endpoints
from flask import Blueprint, jsonify, request
import logging
import sys
import traceback
from datetime import datetime
from libby_backend.cache import cache
from psycopg2.extras import RealDictCursor
from libby_backend.database import count_user_interactions, count_user_interests, get_db_connection

# Import recommendation system
try:
    from libby_backend.recommendation_system import EnhancedBookRecommendationEngine, UserProfile
except ImportError:
    from recommendation_system import EnhancedBookRecommendationEngine, UserProfile

# Initialize recommendation engine
try:
    recommendation_engine = EnhancedBookRecommendationEngine()
except Exception as e:
    logging.error(f"Failed to initialize recommendation engine: {e}")
    recommendation_engine = None

# Create blueprint
rec_bp = Blueprint("recommendations", __name__, url_prefix="/api/recommendations")
logger = logging.getLogger(__name__)

# Allowed interaction types
ALLOWED_TYPES = {"view", "like", "wishlist_add", "wishlist_remove", "rate", "search"}


def build_profile_for(clerk_user_id: str):
    """Build a UserProfile for the given Clerk user id."""
    reading_history: list[int] = []
    wishlist: list[int] = []
    preferred_genres: list[str] = []
    interaction_weights: dict[str, float] = {}

    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Load user's selected genres from user_interests
                cur.execute("""
                    SELECT genre FROM public.user_interests 
                    WHERE clerk_user_id = %s
                """, (clerk_user_id,))
                rows = cur.fetchall() or []
                preferred_genres = [r["genre"] for r in rows if r.get("genre")]
                
                # Load interaction history to build reading_history
                cur.execute("""
                    SELECT book_id, interaction_type 
                    FROM public.user_interactions 
                    WHERE clerk_user_id = %s
                    ORDER BY timestamp DESC
                    LIMIT 100
                """, (clerk_user_id,))
                interactions = cur.fetchall() or []
                
                for interaction in interactions:
                    book_id = interaction.get("book_id")
                    int_type = interaction.get("interaction_type", "")
                    
                    if int_type in ("view", "like", "rate"):
                        if book_id not in reading_history:
                            reading_history.append(book_id)
                    elif int_type in ("wishlist_add",):
                        if book_id not in wishlist:
                            wishlist.append(book_id)
                
                # Build interaction weights based on genre frequency
                if preferred_genres:
                    for genre in preferred_genres:
                        interaction_weights[genre] = 1.0
                        
        finally:
            try:
                conn.close()
            except Exception:
                pass

    profile = UserProfile(
        user_id=str(clerk_user_id),
        email=None,
        selected_genres=preferred_genres,
        reading_history=reading_history,
        wishlist=wishlist,
        interaction_weights=interaction_weights,
        favorite_authors=[],
        preferred_rating_threshold=3.5,
    )
    setattr(profile, 'clerk_user_id', clerk_user_id)
    return profile


def _to_https(u: str | None) -> str | None:
    """Convert http URLs to https"""
    if not u:
        return None
    import re
    return re.sub(r'(?i)^http://', 'https://', u.strip())


@rec_bp.route("/<user_id>/improve", methods=["GET"], endpoint="improve_recs")
def get_improved_recommendations_with_fallbacks(user_id: str):
    try:
        limit = int(request.args.get("limit", 20))

        # Check cache first
        cache_key = f"improve_resp:{user_id}:{limit}"
        cached = None
        try:
            cached = cache.get(cache_key)
        except Exception:
            pass
        if cached:
            return jsonify(cached)

        # Build profile and get recommendations
        profile = build_profile_for(user_id)
        result = recommendation_engine.hybrid_recommendations_enhanced(profile, limit)

        i_count = count_user_interactions(user_id)
        u_count = count_user_interests(user_id)
        algo_label = ("Database Hybrid (Collaborative + Content + Trending)"
                      if (i_count > 0 or u_count > 0) else "Trending (New User)")

        # Prefetch covers from database
        ids = [int(getattr(b, "id")) for b in (result.books or []) if getattr(b, "id", None)]
        cover_by_id = {}
        if ids:
            conn = get_db_connection()
            if conn:
                try:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        placeholders = ','.join(['%s'] * len(ids))
                        cur.execute(f"""
                            SELECT book_id, cover_image_url
                            FROM books
                            WHERE book_id IN ({placeholders})
                        """, ids)
                        rows = cur.fetchall() or []
                        for r in rows:
                            cover_by_id[r["book_id"]] = _to_https(r.get("cover_image_url"))
                finally:
                    try:
                        conn.close()
                    except Exception:
                        pass

        books_data = []
        for book in (result.books or []):
            book_id = getattr(book, "id", None)
            cover_url = cover_by_id.get(int(book_id)) if book_id else None
            if not cover_url:
                cover_url = _to_https(getattr(book, "cover_image_url", None))

            books_data.append({
                "id": book_id,
                "isbn": getattr(book, "isbn", None),
                "title": getattr(book, "title", "Untitled"),
                "author": getattr(book, "author", "Unknown"),
                "genre": getattr(book, "genre", None),
                "description": getattr(book, "description", None),
                "rating": float(getattr(book, "rating", 0.0) or 0.0),
                "cover_image_url": cover_url,
                "publication_date": getattr(book, "publication_date", None),
                "year": getattr(book, "publication_date", None),
                "similarity_score": float(getattr(book, "similarity_score", 0.0) or 0.0),
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

        # Cache response
        try:
            cache.set(cache_key, payload, timeout=300)
        except Exception:
            pass

        return jsonify(payload)

    except Exception as e:
        logger.error(f"Error in /improve for {user_id}: {e}")
        traceback.print_exc(file=sys.stderr)
        return jsonify({
            'success': False,
            'error': 'Failed to generate recommendations',
            'details': str(e)
        }), 500


@rec_bp.route("/interactions/click", methods=["POST", "OPTIONS"])
def record_interaction_click():
    """Record user interactions (clicks, views, wishlist, etc.)"""
    if request.method == "OPTIONS":
        return jsonify({"ok": True}), 200

    data = request.get_json(silent=True) or {}

    # Validate book_id
    try:
        book_id = int(data.get("book_id"))
    except Exception:
        return jsonify({"ok": False, "error": "Invalid book_id"}), 400

    # Validate user
    clerk_user_id = (data.get("clerk_user_id") or data.get("user_id") or "").strip()
    if not clerk_user_id:
        return jsonify({"ok": False, "error": "Missing clerk_user_id"}), 400

    # Normalize interaction type
    interaction_type = (data.get("interaction_type") or data.get("type") or "view").strip()
    if interaction_type == "click":
        interaction_type = "view"
    if interaction_type == "wishlist":
        interaction_type = "wishlist_add"
    if interaction_type not in ALLOWED_TYPES:
        return jsonify({"ok": False, "error": f"Invalid type: {interaction_type}"}), 400

    # Process rating
    rating_raw = data.get("rating")
    rating = None
    if rating_raw not in (None, "", "null"):
        try:
            rating = float(rating_raw)
        except Exception:
            pass

    # Verify book exists
    conn = None
    try:
        conn = get_db_connection()
        if conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM books WHERE book_id = %s LIMIT 1", (book_id,))
                if not cur.fetchone():
                    return jsonify({"ok": False, "error": "Book not found"}), 404
    except Exception as e:
        logger.error(f"Error checking book existence: {e}")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    # Insert interaction
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"ok": False, "error": "Database unavailable"}), 503

        with conn.cursor() as cur:
            # Resolve or create user_id from clerk_user_id
            cur.execute("""
                SELECT user_id FROM public.users WHERE clerk_user_id = %s LIMIT 1
            """, (clerk_user_id,))
            user_row = cur.fetchone()
            
            if user_row:
                user_id = user_row['user_id']
            else:
                # Create a deterministic user_id from clerk_user_id hash
                user_id = abs(hash(clerk_user_id)) % (10 ** 9)
            
            # Insert with both clerk_user_id and user_id
            cur.execute("""
                INSERT INTO user_interactions 
                (clerk_user_id, user_id, book_id, interaction_type, rating, timestamp)
                VALUES (%s, %s, %s, %s, %s, NOW())
            """, (clerk_user_id, user_id, book_id, interaction_type, rating))
            
            conn.commit()

        conn.close()
        return jsonify({"ok": True, "message": "Interaction recorded"}), 200

    except Exception as e:
        logger.error(f"Error recording interaction: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500


@rec_bp.route("/health", methods=["GET"])
def recommendation_health_check():
    """Health check for recommendation system"""
    try:
        status = {
            "status": "healthy",
            "engine_available": recommendation_engine is not None,
            "timestamp": datetime.now().isoformat()
        }

        # Test database connection
        try:
            conn = get_db_connection()
            if conn:
                conn.close()
                status["database"] = "connected"
            else:
                status["database"] = "unavailable"
        except Exception as e:
            status["database"] = f"error: {str(e)}"

        return jsonify(status), 200

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            "status": "unhealthy",
            "error": str(e)
        }), 500
