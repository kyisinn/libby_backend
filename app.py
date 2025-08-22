# =============================================================================
# FLASK BOOK RECOMMENDATION API - ENTRYPOINT (CLEANED)
# =============================================================================
from flask import Flask, jsonify
from flask_cors import CORS
from cache import init_cache

# =============================================================================
# APPLICATION SETUP
# =============================================================================
app = Flask(__name__)
CORS(
    app,
    resources={
        r"/api/*": {
            "origins": ["https://libby-bot.vercel.app"],
            "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
            "allow_headers": ["Authorization", "Content-Type", "X-Requested-With"],
            "expose_headers": ["Content-Type"],
        }
    },
    supports_credentials=True,
)
cache = init_cache(app)

# =============================================================================
# OPTIONAL BLUEPRINT REGISTRATION
# (These will be used if you've split your routes into modules.)
# =============================================================================
try:
    from blueprints.auth.routes import auth_bp
    app.register_blueprint(auth_bp, url_prefix="/api/auth")
except Exception:
    pass

try:
    from blueprints.books.routes import books_bp
    app.register_blueprint(books_bp, url_prefix="/api/books")
except Exception:
    pass

# try:
#     from blueprints.recommendations.routes import rec_bp
#     app.register_blueprint(rec_bp, url_prefix="/api/recommendations")
# except Exception:
#     pass

try:
    from blueprints.health.routes import health_bp
    app.register_blueprint(health_bp, url_prefix="/api/health")
except Exception:
    pass

# =============================================================================
# HEALTH CHECK (fallback if no health blueprint is present)
# =============================================================================
@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({
        "status": "healthy",
        "service": "book-recommendation-api",
        "version": "1.0.0"
    })

@app.route('/api/health/detailed', methods=['GET'])
def detailed_health_check():
    try:
        from database import get_db_connection
        conn = get_db_connection()
        db_ok = bool(conn)
        if conn:
            conn.close()

        cache_ok = True
        if app.config.get("CACHE_TYPE") == "RedisCache":
            try:
                cache.set("health:ping", "pong", timeout=5)
                cache_ok = cache.get("health:ping") == "pong"
            except Exception:
                cache_ok = False

        status = "healthy" if (db_ok and cache_ok) else "degraded"
        return jsonify({
            "status": status,
            "database": "connected" if db_ok else "failed",
            "cache": "connected" if cache_ok else "failed",
        })
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 500

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