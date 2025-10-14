from flask import Blueprint, jsonify, request
from libby_backend.database import (
        search_books_db, get_trending_books_db, get_books_by_major_db, get_book_by_id_db,
        save_user_rating_db, get_user_rating_db, get_user_ratings_db, 
        get_book_ratings_db, delete_user_rating_db, get_book_rating_stats_db
)
from libby_backend.utils.user_resolver import resolve_user_id
from ...extensions import cache
import logging

logger = logging.getLogger(__name__)

books_bp = Blueprint("books", __name__, url_prefix="/api/books")

@books_bp.get("/search")
@cache.cached(timeout=30, query_string=True)  # Reduced from 180 to 30 seconds

def search_books():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify([])
    results = search_books_db(q)
    if results is None:
        return jsonify({"error": "Database connection failed."}), 500
    return jsonify(results)


@books_bp.get("/recommendations/globally-trending")
@cache.cached(timeout=60, query_string=True)  # Reduced from 600 to 60 seconds
def globally_trending():
    period = request.args.get("period", "5years", type=str)
    page   = request.args.get("page", 1, type=int)
    per_page = 20
    result = get_trending_books_db(period, page, per_page)
    if result is None:
        return jsonify({"error": "Database connection failed."}), 500
    return jsonify({
        "books": result["books"],
        "total_books": result["total_books"],
        "page": page,
        "per_page": per_page,
    })

@books_bp.get("/recommendations/by-major")
@cache.cached(timeout=60, query_string=True)  # Reduced from 600 to 60 seconds
def by_major():
    major = request.args.get("major", "Computer Science", type=str)
    page  = request.args.get("page", 1, type=int)
    per_page = 15
    result = get_books_by_major_db(major, page, per_page)
    if result is None:
        return jsonify({"error": "Database connection failed."}), 500
    return jsonify({
        "books": result["books"],
        "total_books": result["total_books"],
        "page": page,
        "per_page": per_page,
    })

@books_bp.get("/recommendations/similar-to/<int:book_id>")
def similar_to(book_id: int):
    # placeholder (kept minimal as in your code)
    target = get_book_by_id_db(book_id)
    if not target:
        return jsonify({"error": "Book not found."}), 404
    return jsonify([])


@books_bp.get("/books/<int:book_id>")
@cache.cached(timeout=1800)
def get_book(book_id: int):
    book = get_book_by_id_db(book_id)
    if book is None:
        return jsonify({"error": "Database connection failed."}), 500
    if not book:
        return jsonify({"error": "Book not found"}), 404
    return jsonify(dict(book))


# =============================================================================
# RATING ENDPOINTS
# =============================================================================

@books_bp.route("/ratings", methods=["POST"])
def create_or_update_rating():
    """Create or update a user rating for a book."""
    data = request.get_json(silent=True) or {}
    
    # Validate required fields
    clerk_user_id = data.get("clerk_user_id") or data.get("user_id")
    book_id = data.get("book_id")
    rating = data.get("rating")
    review_text = data.get("review_text")
    
    if not clerk_user_id:
        return jsonify({"error": "user_id or clerk_user_id is required"}), 400
    if not book_id:
        return jsonify({"error": "book_id is required"}), 400
    if rating is None:
        return jsonify({"error": "rating is required"}), 400
    
    # Validate rating range (1-5)
    try:
        rating = float(rating)
        if rating < 1 or rating > 5:
            return jsonify({"error": "rating must be between 1 and 5"}), 400
    except (ValueError, TypeError):
        return jsonify({"error": "rating must be a number"}), 400
    
    # Resolve user_id
    try:
        user_id = resolve_user_id(clerk_user_id)
    except Exception as e:
        logger.error(f"Error resolving user_id: {e}")
        return jsonify({"error": "Failed to resolve user"}), 500
    
    # Save rating (pass clerk_user_id for user creation if needed)
    result = save_user_rating_db(user_id, int(book_id), rating, review_text, clerk_user_id)
    if result is None:
        return jsonify({"error": "Failed to save rating"}), 500
    
    # Convert Decimal to float for the rating field
    if result and 'rating' in result and result['rating'] is not None:
        result['rating'] = float(result['rating'])
    
    return jsonify({
        "success": True,
        "rating": result
    }), 201


@books_bp.route("/ratings/<int:book_id>", methods=["GET"])
@cache.cached(timeout=60, query_string=True)
def get_book_ratings(book_id: int):
    """Get all ratings for a specific book."""
    limit = request.args.get("limit", 50, type=int)
    
    ratings = get_book_ratings_db(book_id, limit)
    stats = get_book_rating_stats_db(book_id)
    
    # Convert Decimal to float for rating fields
    for rating in ratings:
        if rating and 'rating' in rating and rating['rating'] is not None:
            rating['rating'] = float(rating['rating'])
    
    # Convert stats decimals to float
    if stats:
        if 'average_rating' in stats and stats['average_rating'] is not None:
            stats['average_rating'] = float(stats['average_rating'])
    
    return jsonify({
        "success": True,
        "book_id": book_id,
        "ratings": ratings,
        "stats": stats
    })


@books_bp.route("/ratings/<int:book_id>/stats", methods=["GET"])
@cache.cached(timeout=300)
def get_rating_stats(book_id: int):
    """Get rating statistics for a book."""
    stats = get_book_rating_stats_db(book_id)
    if stats is None:
        return jsonify({"error": "Failed to get rating stats"}), 500
    
    return jsonify({
        "success": True,
        "stats": stats
    })


@books_bp.route("/ratings/user/<clerk_user_id>", methods=["GET"])
def get_user_all_ratings(clerk_user_id: str):
    """Get all ratings by a specific user."""
    limit = request.args.get("limit", 50, type=int)
    
    try:
        user_id = resolve_user_id(clerk_user_id)
    except Exception as e:
        logger.error(f"Error resolving user_id: {e}")
        return jsonify({"error": "Failed to resolve user"}), 500
    
    ratings = get_user_ratings_db(user_id, limit)
    
    # Convert Decimal to float for rating fields
    for rating in ratings:
        if rating and 'rating' in rating and rating['rating'] is not None:
            rating['rating'] = float(rating['rating'])
    
    return jsonify({
        "success": True,
        "user_id": clerk_user_id,
        "ratings": ratings,
        "total": len(ratings)
    })


@books_bp.route("/ratings/user/<clerk_user_id>/book/<int:book_id>", methods=["GET"])
def get_user_book_rating(clerk_user_id: str, book_id: int):
    """Get a specific user's rating for a specific book."""
    try:
        user_id = resolve_user_id(clerk_user_id)
    except Exception as e:
        logger.error(f"Error resolving user_id: {e}")
        return jsonify({"error": "Failed to resolve user"}), 500
    
    rating = get_user_rating_db(user_id, book_id)
    
    if rating is None:
        return jsonify({
            "success": True,
            "has_rating": False,
            "rating": None
        })
    
    # Convert Decimal to float for the rating field
    if rating and 'rating' in rating and rating['rating'] is not None:
        rating['rating'] = float(rating['rating'])
    
    return jsonify({
        "success": True,
        "has_rating": True,
        "rating": rating
    })


@books_bp.route("/ratings/user/<clerk_user_id>/book/<int:book_id>", methods=["DELETE"])
def delete_user_book_rating(clerk_user_id: str, book_id: int):
    """Delete a user's rating for a book."""
    try:
        user_id = resolve_user_id(clerk_user_id)
    except Exception as e:
        logger.error(f"Error resolving user_id: {e}")
        return jsonify({"error": "Failed to resolve user"}), 500
    
    success = delete_user_rating_db(user_id, book_id)
    
    if not success:
        return jsonify({"error": "Failed to delete rating"}), 500
    
    return jsonify({
        "success": True,
        "message": "Rating deleted successfully"
    })