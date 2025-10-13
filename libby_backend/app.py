# =============================================================================
# FLASK BOOK RECOMMENDATION API - ENTRYPOINT (CLEANED WITH CLERK & CACHE)
# =============================================================================
from flask import Flask, jsonify
from flask_cors import CORS
import os, re

# Clerk authentication routes
from libby_backend.blueprints.clerk.routes import clerk_bp

# Cache system (Redis or SimpleCache)
from libby_backend.cache import init_cache

# Database connection for health check
from libby_backend.database import get_db_connection

# Hybrid recommender engine
from libby_backend.recommender.hybrid_fusion import get_final_recommendations

# App configuration
from libby_backend.config import Config


# =============================================================================
# APPLICATION SETUP
# =============================================================================
app = Flask(__name__)
app.config.from_object(Config)

# Register Clerk blueprint for authentication routes
app.register_blueprint(clerk_bp)

# Initialize cache
cache = init_cache(app)


# =============================================================================
# CORS CONFIGURATION
# =============================================================================
VERCEL_ORIGIN = "https://libby-bot.vercel.app"
ALLOWED_ORIGINS = [
    VERCEL_ORIGIN,
    "http://localhost:3000",
    "http://127.0.0.1:3000"
]
# Allow all Vercel subdomains (e.g., staging builds)
origin_regex = re.compile(r"^https://[-a-z0-9]+\.vercel\.app$")

CORS(
    app,
    resources={r"/api/*": {"origins": ALLOWED_ORIGINS + [origin_regex]}},
    methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Requested-With"],
    expose_headers=["Content-Length", "Content-Type"],
    max_age=86400,
    supports_credentials=False,  # set True ONLY if you send cookies
)


# =============================================================================
# HEALTH CHECK
# =============================================================================
@app.route('/api/health', methods=['GET'])
def health_check():
    """Check database and cache status."""
    try:
        # Check database
        conn = get_db_connection()
        db_ok = bool(conn)
        if conn:
            conn.close()

        # Check cache
        cache_ok = True
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
            "service": "libby-bot-recommender",
            "version": "1.0.0"
        })
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 500


# =============================================================================
# RECOMMENDATION ENDPOINT
# =============================================================================
@app.route('/api/recommend/<int:user_id>', methods=['GET'])
def recommend(user_id):
    """Generate hybrid recommendations for a user (cached)."""
    cache_key = f"recommend:{user_id}"
    cached_result = cache.get(cache_key)
    if cached_result:
        return jsonify({"cached": True, "data": cached_result})

    try:
        results = get_final_recommendations(user_id, top_n=10)
        cache.set(cache_key, results, timeout=300)  # cache for 5 minutes
        return jsonify({"cached": False, "data": results})
    except Exception as e:
        return jsonify({"error": f"Failed to generate recommendations: {str(e)}"}), 500


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
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=True)