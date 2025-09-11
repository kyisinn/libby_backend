# blueprints/recommendations/routes.py
from flask import Blueprint, jsonify, request
from libby_backend.recommendation_system import BookRecommendationEngine, RecommendationAPI
from ...extensions import cache
import logging

# Initialize recommendation system
recommendation_engine = BookRecommendationEngine()
recommendation_api = RecommendationAPI(recommendation_engine)

# Create blueprint
rec_bp = Blueprint("recommendations", __name__, url_prefix="/api/recommendations")

logger = logging.getLogger(__name__)

@rec_bp.route("/<user_id>", methods=["GET"])
@cache.cached(timeout=300, query_string=True)  # Cache for 5 minutes
def get_user_recommendations(user_id: str):
    """Get personalized recommendations for a user"""
    try:
        email = request.args.get('email')
        genres = request.args.getlist('genres')  # Can pass multiple genres
        limit = int(request.args.get('limit', 20))
        
        result = recommendation_api.get_user_recommendations(
            user_id=user_id,
            email=email,
            genres=genres if genres else None,
            limit=limit
        )
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error getting recommendations for {user_id}: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to generate recommendations',
            'books': []
        }), 500

@rec_bp.route("/<user_id>/trending", methods=["GET"])
@cache.cached(timeout=600, query_string=True)  # Cache for 10 minutes
def get_personalized_trending(user_id: str):
    """Get trending books personalized for the user"""
    try:
        limit = int(request.args.get('limit', 10))
        
        result = recommendation_api.get_personalized_trending(
            user_id=user_id,
            limit=limit
        )
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error getting personalized trending for {user_id}: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to get personalized trending books',
            'books': []
        }), 500

@rec_bp.route("/<user_id>/refresh", methods=["POST"])
def refresh_user_recommendations(user_id: str):
    """Force refresh recommendations for a user"""
    try:
        limit = int(request.args.get('limit', 20))
        
        # Clear cache for this user
        cache_key = f"view/api/recommendations/{user_id}"
        cache.delete(cache_key)
        
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
def record_user_interaction():
    """Record a user interaction with a book"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                'success': False,
                'error': 'No data provided'
            }), 400
        
        user_id = data.get('user_id')
        book_id = data.get('book_id')
        interaction_type = data.get('type')  # 'view', 'wishlist_add', 'wishlist_remove', 'search', 'click', 'like', 'dislike'
        
        if not all([user_id, book_id, interaction_type]):
            return jsonify({
                'success': False,
                'error': 'Missing required fields: user_id, book_id, type'
            }), 400
        
        # Validate interaction type
        valid_types = ['view', 'wishlist_add', 'wishlist_remove', 'search', 'click', 'like', 'dislike']
        if interaction_type not in valid_types:
            return jsonify({
                'success': False,
                'error': f'Invalid interaction type. Must be one of: {", ".join(valid_types)}'
            }), 400
        
        result = recommendation_api.record_interaction(
            user_id=str(user_id),
            book_id=int(book_id),
            interaction_type=interaction_type
        )
        
        # Clear user's recommendation cache after interaction
        if result.get('success'):
            cache_key = f"view/api/recommendations/{user_id}"
            cache.delete(cache_key)
        
        return jsonify(result)
        
    except ValueError as e:
        return jsonify({
            'success': False,
            'error': 'Invalid book_id format'
        }), 400
    except Exception as e:
        logger.error(f"Error recording interaction: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to record interaction'
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
@cache.cached(timeout=1800, query_string=True)  # Cache for 30 minutes
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
@cache.cached(timeout=900, query_string=True)  # Cache for 15 minutes
def get_recommendations_by_genre():
    """Get recommendations for specific genres"""
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
            genre_books = recommendation_engine._fetch_books_by_genre(
                genre=genre,
                limit=books_per_genre * 2  # Get extra for filtering
            )
            
            # If user provided, personalize the selection
            if user_id and genre_books:
                profile = recommendation_engine.get_user_profile(user_id)
                exclude_ids = profile.reading_history + profile.wishlist
                
                # Filter out books user has already interacted with
                filtered_books = [book for book in genre_books if book.id not in exclude_ids]
                genre_books = filtered_books[:books_per_genre]
            else:
                genre_books = genre_books[:books_per_genre]
            
            all_books.extend(genre_books)
        
        # Remove duplicates
        seen_ids = set()
        unique_books = []
        for book in all_books:
            if book.id not in seen_ids:
                seen_ids.add(book.id)
                unique_books.append(book.__dict__)
        
        return jsonify({
            'success': True,
            'books': unique_books[:limit],
            'genres': genres,
            'total_count': len(unique_books)
        })
        
    except Exception as e:
        logger.error(f"Error getting recommendations by genre: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to get genre recommendations'
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
    """Health check for recommendation system"""
    try:
        # Test database connections
        from libby_backend.database import get_db_connection
        import sqlite3
        
        # Test main database
        main_db_ok = False
        try:
            conn = get_db_connection()
            if conn:
                conn.close()
                main_db_ok = True
        except:
            pass
        
        # Test recommendation database
        rec_db_ok = False
        try:
            conn = sqlite3.connect(recommendation_engine.db_path)
            conn.close()
            rec_db_ok = True
        except:
            pass
        
        # Test basic functionality
        test_books = recommendation_engine._fetch_trending_books(5)
        functionality_ok = len(test_books) > 0
        
        status = "healthy" if (main_db_ok and rec_db_ok and functionality_ok) else "unhealthy"
        
        return jsonify({
            'status': status,
            'main_database': 'connected' if main_db_ok else 'failed',
            'recommendation_database': 'connected' if rec_db_ok else 'failed',
            'basic_functionality': 'working' if functionality_ok else 'failed',
            'test_books_fetched': len(test_books) if functionality_ok else 0
        })
        
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e)
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


# USER INTERACTIONS
@rec_bp.route("/interactions/view", methods=["POST"])
def record_view():
    data = request.json
    user_id = data.get("user_id")
    book_id = data.get("book_id")

    if not user_id or not book_id:
        return jsonify({"error": "Missing user_id or book_id"}), 400

    result = record_user_interaction(user_id, book_id, "view")
    if result:
        return jsonify({"status": "success", "data": result}), 201
    else:
        return jsonify({"status": "error", "message": "Failed to record view"}), 500


@rec_bp.route("/test", methods=["GET"])
def test_recommendation_system():
    """Quick remote test to verify DB integration + recommendation engine"""
    try:
        from libby_backend.recommendation_system import test_recommendation_system_with_db

        result = test_recommendation_system_with_db()

        return jsonify({
            "status": "success",
            "message": "Database-Integrated Recommendation System is working!",
            "sample_result": result
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
