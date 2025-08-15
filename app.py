# =============================================================================
# FLASK BOOK RECOMMENDATION API - MAIN BACKEND
# =============================================================================
# Main Flask application that handles all API endpoints and routing

from flask import Flask, jsonify, request
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash

# Import our custom modules
from database import (
    search_books_db, 
    get_trending_books_db, 
    get_books_by_major_db, 
    get_book_by_id_db,
    get_similar_books_details
)
from cache import (
    calculate_similar_books, 
    refresh_similarity_cache, 
    get_cache_stats,
    get_cached_search,
    cache_search_results
)

# =============================================================================
# APPLICATION SETUP
# =============================================================================

app = Flask(__name__)
CORS(app)

# =============================================================================
# USER AUTHENTICATION ENDPOINTS
# =============================================================================
# Note: Authentication endpoints would go here
# (These remain the same as in your original code)

# =============================================================================
# BOOK SEARCH ENDPOINTS
# =============================================================================

@app.route('/api/search', methods=['GET'])
def search_books():
    """Searches for books and returns a simple list with caching."""
    query = request.args.get('q', '').strip()
    if not query: 
        return jsonify([])
    
    # Check cache first
    cached_results = get_cached_search(query)
    if cached_results is not None:
        return jsonify(cached_results)
    
    # Get from database
    results = search_books_db(query)
    if results is None:
        return jsonify({"error": "Database connection failed."}), 500
    
    # Cache the results
    cache_search_results(query, results)
    
    return jsonify(results)

# =============================================================================
# BOOK RECOMMENDATION ENDPOINTS
# =============================================================================

@app.route('/api/recommendations/globally-trending', methods=['GET'])
def get_globally_trending():
    """Gets top books by time period with pagination."""
    period = request.args.get('period', '5years', type=str)
    page = request.args.get('page', 1, type=int)
    per_page = 20
    
    result = get_trending_books_db(period, page, per_page)
    if result is None:
        return jsonify({"error": "Database connection failed."}), 500
    
    return jsonify({
        'books': result['books'],
        'total_books': result['total_books'],
        'page': page,
        'per_page': per_page
    })

@app.route('/api/recommendations/by-major', methods=['GET'])
def get_by_major():
    """Gets books by major with pagination."""
    major = request.args.get('major', 'Computer Science', type=str)
    page = request.args.get('page', 1, type=int)
    per_page = 15
    
    result = get_books_by_major_db(major, page, per_page)
    if result is None:
        return jsonify({"error": "Database connection failed."}), 500
    
    return jsonify({
        'books': result['books'],
        'total_books': result['total_books'],
        'page': page,
        'per_page': per_page
    })

# =============================================================================
# CONTENT-BASED SIMILARITY RECOMMENDATIONS
# =============================================================================

@app.route('/api/recommendations/similar-to/<int:book_id>', methods=['GET'])
def get_similar_books(book_id):
    """Gets content-based recommendations using cached similarity calculations."""
    try:
        similar_book_ids = calculate_similar_books(book_id)
        
        if similar_book_ids is None:
            return jsonify({"error": "Book not found in dataset for similarity calculation."}), 404
        
        if not similar_book_ids:
            return jsonify([])

        # Get full details for the recommended books
        results = get_similar_books_details(similar_book_ids)
        if results is None:
            return jsonify({"error": "Database connection failed."}), 500
        
        return jsonify(results)

    except Exception as e:
        print(f"Error in get_similar_books: {e}")
        return jsonify({"error": "An internal error occurred."}), 500

# =============================================================================
# INDIVIDUAL BOOK ENDPOINTS
# =============================================================================

@app.route('/api/books/<int:book_id>', methods=['GET'])
def get_book_by_id(book_id):
    """Gets all details for a single book by its ID."""
    book = get_book_by_id_db(book_id)
    
    if book is None:
        return jsonify({"error": "Database connection failed."}), 500
    
    if book:
        # Transform the response format
        book = dict(book)
        book['id'] = book.pop('book_id')
        book['coverurl'] = book.pop('cover_image_url')
        return jsonify(book)
    else:
        return jsonify({"error": "Book not found"}), 404

# =============================================================================
# CACHE MANAGEMENT ENDPOINTS
# =============================================================================

@app.route('/api/admin/cache/refresh', methods=['POST'])
def refresh_cache():
    """Refresh the similarity calculation cache."""
    try:
        refresh_similarity_cache()
        return jsonify({"message": "Cache refreshed successfully"})
    except Exception as e:
        print(f"Error refreshing cache: {e}")
        return jsonify({"error": "Failed to refresh cache"}), 500

@app.route('/api/admin/cache/stats', methods=['GET'])
def cache_stats():
    """Get cache statistics."""
    try:
        stats = get_cache_stats()
        return jsonify(stats)
    except Exception as e:
        print(f"Error getting cache stats: {e}")
        return jsonify({"error": "Failed to get cache stats"}), 500

# =============================================================================
# HEALTH CHECK ENDPOINTS
# =============================================================================

@app.route('/api/health', methods=['GET'])
def health_check():
    """Basic health check endpoint."""
    return jsonify({
        "status": "healthy",
        "service": "book-recommendation-api",
        "version": "1.0.0"
    })

@app.route('/api/health/detailed', methods=['GET'])
def detailed_health_check():
    """Detailed health check with database connectivity."""
    try:
        # Test database connection
        from database import get_db_connection
        conn = get_db_connection()
        if conn:
            conn.close()
            db_status = "connected"
        else:
            db_status = "failed"
        
        # Get cache stats
        cache_stats_data = get_cache_stats()
        
        return jsonify({
            "status": "healthy" if db_status == "connected" else "degraded",
            "service": "book-recommendation-api",
            "version": "1.0.0",
            "database": db_status,
            "cache": cache_stats_data
        })
    except Exception as e:
        print(f"Health check error: {e}")
        return jsonify({
            "status": "unhealthy",
            "error": str(e)
        }), 500

# =============================================================================
# ERROR HANDLERS
# =============================================================================

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

@app.errorhandler(400)
def bad_request(error):
    return jsonify({"error": "Bad request"}), 400

# =============================================================================
# APPLICATION RUNNER
# =============================================================================

if __name__ == '__main__':
    app.run(debug=True)