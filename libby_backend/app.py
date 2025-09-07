# =============================================================================
# FLASK BOOK RECOMMENDATION API - ENTRYPOINT (WITH RECOMMENDATION SYSTEM)
# =============================================================================
from flask import Flask, jsonify
from flask_cors import CORS
from libby_backend.cache import init_cache
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# APPLICATION SETUP
# =============================================================================
app = Flask(__name__)
CORS(
    app,
    resources={
        r"/api/*": {
            "origins": ["https://libby-bot.vercel.app", "http://localhost:3000"],
            "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
            "allow_headers": ["Authorization", "Content-Type", "X-Requested-With"],
            "expose_headers": ["Content-Type"],
        }
    },
    supports_credentials=True,
)
cache = init_cache(app)

# =============================================================================
# BLUEPRINT REGISTRATION
# =============================================================================

# Auth routes
try:
    from blueprints.auth.routes import auth_bp
    app.register_blueprint(auth_bp, url_prefix="/api/auth")
    logger.info("Auth blueprint registered")
except ImportError as e:
    logger.warning(f"Could not import auth blueprint: {e}")

# Books routes
try:
    from blueprints.books.routes import books_bp
    app.register_blueprint(books_bp, url_prefix="/api/books")
    logger.info("Books blueprint registered")
except ImportError as e:
    logger.warning(f"Could not import books blueprint: {e}")

# Recommendation routes (NEW)
try:
    from blueprints.recommendations.routes import rec_bp
    app.register_blueprint(rec_bp, url_prefix="/api/recommendations")
    logger.info("Recommendations blueprint registered")
except ImportError as e:
    logger.warning(f"Could not import recommendations blueprint: {e}")

# Health routes
try:
    from blueprints.health.routes import health_bp
    app.register_blueprint(health_bp, url_prefix="/api/health")
    logger.info("Health blueprint registered")
except ImportError as e:
    logger.warning(f"Could not import health blueprint: {e}")

# Profile routes
try:
    from blueprints.profile.routes import profile_bp
    app.register_blueprint(profile_bp, url_prefix="/api/profile")
    logger.info("Profile blueprint registered")
except ImportError as e:
    logger.warning(f"Could not import profile blueprint: {e}")

# =============================================================================
# HEALTH CHECK (fallback if no health blueprint is present)
# =============================================================================
@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({
        "status": "healthy",
        "service": "book-recommendation-api",
        "version": "2.0.0",
        "features": [
            "book_search",
            "trending_books", 
            "personalized_recommendations",
            "user_interactions",
            "collaborative_filtering"
        ]
    })

@app.route('/api/health/detailed', methods=['GET'])
def detailed_health_check():
    try:
        from libby_backend.database import get_db_connection
        import sqlite3
        
        # Test main database
        main_db_ok = False
        try:
            conn = get_db_connection()
            if conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                conn.close()
                main_db_ok = True
        except Exception as e:
            logger.error(f"Main database check failed: {e}")
        
        # Test recommendation database
        rec_db_ok = False
        try:
            from libby_backend.recommendation_system import BookRecommendationEngine
            engine = BookRecommendationEngine()
            conn = sqlite3.connect(engine.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            conn.close()
            rec_db_ok = True
        except Exception as e:
            logger.error(f"Recommendation database check failed: {e}")
        
        # Test cache
        cache_ok = True
        if app.config.get("CACHE_TYPE") == "RedisCache":
            try:
                cache.set("health:ping", "pong", timeout=5)
                cache_ok = cache.get("health:ping") == "pong"
            except Exception as e:
                logger.error(f"Cache check failed: {e}")
                cache_ok = False
        
        # Test recommendation functionality
        rec_functionality_ok = False
        try:
            from libby_backend.recommendation_system import BookRecommendationEngine
            engine = BookRecommendationEngine()
            test_books = engine._fetch_trending_books(5)
            rec_functionality_ok = len(test_books) > 0
        except Exception as e:
            logger.error(f"Recommendation functionality check failed: {e}")
        
        overall_status = "healthy" if all([main_db_ok, rec_db_ok, cache_ok, rec_functionality_ok]) else "degraded"
        
        return jsonify({
            "status": overall_status,
            "components": {
                "main_database": "connected" if main_db_ok else "failed",
                "recommendation_database": "connected" if rec_db_ok else "failed", 
                "cache": "connected" if cache_ok else "failed",
                "recommendation_engine": "working" if rec_functionality_ok else "failed"
            },
            "timestamp": app.config.get('startup_time', 'unknown')
        })
        
    except Exception as e:
        logger.error(f"Detailed health check error: {e}")
        return jsonify({
            "status": "unhealthy", 
            "error": str(e),
            "components": {
                "main_database": "unknown",
                "recommendation_database": "unknown",
                "cache": "unknown",
                "recommendation_engine": "unknown"
            }
        }), 500

# =============================================================================
# RECOMMENDATION SYSTEM INITIALIZATION
# =============================================================================
@app.before_first_request
def initialize_recommendation_system():
    """Initialize the recommendation system on first request"""
    try:
        from libby_backend.recommendation_system import BookRecommendationEngine
        
        logger.info("Initializing recommendation system...")
        engine = BookRecommendationEngine()
        
        # Test basic functionality
        test_books = engine._fetch_trending_books(5)
        logger.info(f"Recommendation system initialized successfully. Test fetch: {len(test_books)} books")
        
        # Store startup time
        from datetime import datetime
        app.config['startup_time'] = datetime.now().isoformat()
        
    except Exception as e:
        logger.error(f"Failed to initialize recommendation system: {e}")

# =============================================================================
# ENHANCED ERROR HANDLERS
# =============================================================================
@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "error": "Endpoint not found",
        "available_endpoints": [
            "/api/health",
            "/api/books/search",
            "/api/books/recommendations/globally-trending",
            "/api/recommendations/<user_id>",
            "/api/recommendations/interactions"
        ]
    }), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal server error: {error}")
    return jsonify({
        "error": "Internal server error",
        "message": "Please try again later"
    }), 500

@app.errorhandler(400)
def bad_request(error):
    return jsonify({
        "error": "Bad request",
        "message": "Please check your request parameters"
    }), 400

@app.errorhandler(429)
def rate_limit_error(error):
    return jsonify({
        "error": "Rate limit exceeded",
        "message": "Too many requests, please try again later"
    }), 429

# =============================================================================
# MIDDLEWARE FOR REQUEST LOGGING
# =============================================================================
@app.before_request
def log_request_info():
    """Log request information for debugging"""
    if app.debug:
        logger.info(f"Request: {request.method} {request.path}")
        if request.args:
            logger.info(f"Query params: {dict(request.args)}")

@app.after_request
def log_response_info(response):
    """Log response information for debugging"""
    if app.debug:
        logger.info(f"Response: {response.status_code}")
    return response

# =============================================================================
# ENHANCED CORS ERROR HANDLER
# =============================================================================
@app.after_request
def after_request(response):
    """Enhanced CORS handling"""
    origin = request.headers.get('Origin')
    
    # Allow requests from development environments
    allowed_origins = [
        'https://libby-bot.vercel.app',
        'http://localhost:3000',
        'http://127.0.0.1:3000'
    ]
    
    if origin in allowed_origins:
        response.headers.add('Access-Control-Allow-Origin', origin)
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-Requested-With')
        response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
        response.headers.add('Access-Control-Allow-Credentials', 'true')
    
    return response

# =============================================================================
# RECOMMENDATION INTEGRATION ENDPOINTS (Direct integration)
# =============================================================================

# If recommendation blueprint fails to load, provide fallback endpoints
try:
    from blueprints.recommendations.routes import rec_bp
except ImportError:
    logger.warning("Recommendation blueprint not found, creating fallback endpoints")
    
    @app.route('/api/recommendations/<user_id>', methods=['GET'])
    def fallback_get_recommendations(user_id):
        """Fallback recommendation endpoint"""
        try:
            from libby_backend.recommendation_system import BookRecommendationEngine, RecommendationAPI
            
            engine = BookRecommendationEngine()
            api = RecommendationAPI(engine)
            
            limit = int(request.args.get('limit', 20))
            result = api.get_user_recommendations(user_id=user_id, limit=limit)
            
            return jsonify(result)
            
        except Exception as e:
            logger.error(f"Fallback recommendation error: {e}")
            return jsonify({
                'success': False,
                'error': 'Recommendation service unavailable',
                'books': []
            }), 500
    
    @app.route('/api/recommendations/interactions', methods=['POST'])
    def fallback_record_interaction():
        """Fallback interaction recording endpoint"""
        try:
            from libby_backend.recommendation_system import BookRecommendationEngine, RecommendationAPI
            
            data = request.get_json()
            if not data:
                return jsonify({'success': False, 'error': 'No data provided'}), 400
            
            engine = BookRecommendationEngine()
            api = RecommendationAPI(engine)
            
            result = api.record_interaction(
                user_id=data.get('user_id'),
                book_id=data.get('book_id'),
                interaction_type=data.get('type')
            )
            
            return jsonify(result)
            
        except Exception as e:
            logger.error(f"Fallback interaction error: {e}")
            return jsonify({
                'success': False,
                'error': 'Interaction service unavailable'
            }), 500

# =============================================================================
# APPLICATION RUNNER
# =============================================================================
if __name__ == '__main__':
    # Set debug mode based on environment
    import os
    debug_mode = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    
    logger.info("Starting Flask application with recommendation system...")
    logger.info(f"Debug mode: {debug_mode}")
    
    app.run(
        debug=debug_mode,
        host='0.0.0.0',
        port=int(os.getenv('PORT', 5000))
    )