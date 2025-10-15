# =============================================================================
# FLASK BOOK RECOMMENDATION API - ENTRYPOINT (CLEANED)
# =============================================================================
from flask import Flask, jsonify
from flask_cors import CORS
from libby_backend.cache import init_cache
import os, re

from libby_backend.blueprints.clerk.routes import clerk_bp
from apscheduler.schedulers.background import BackgroundScheduler
import atexit

# Notify prefs blueprint and digest runner
from libby_backend.notify_prefs_routes import prefs_bp
from libby_backend.digests import send_due_digests_batch



# =============================================================================
# APPLICATION SETUP
# =============================================================================
from libby_backend.config import Config
from libby_backend.extensions import mail

app = Flask(__name__)
app.config.from_object(Config)

# Configure Flask-Mail
app.config['MAIL_SERVER'] = os.getenv('SMTP_HOST', 'smtp.gmail.com')
app.config['MAIL_PORT'] = int(os.getenv('SMTP_PORT', '587'))
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USE_SSL'] = False
app.config['MAIL_USERNAME'] = os.getenv('SENDER_EMAIL')
app.config['MAIL_PASSWORD'] = os.getenv('SENDER_APP_PASSWORD')
app.config['MAIL_DEFAULT_SENDER'] = os.getenv('SENDER_EMAIL')

# Initialize Flask-Mail
mail.init_app(app)

app.register_blueprint(clerk_bp)

VERCEL_ORIGIN = "https://libby-bot.vercel.app"
ALLOWED_ORIGINS = [
    VERCEL_ORIGIN,
    "http://localhost:3000",
    "http://127.0.0.1:3000"
]
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
cache = init_cache(app)

# Initialize database tables for recommendation system
from libby_backend.database import initialize_recommendation_tables
try:
    initialize_recommendation_tables()
except Exception as e:
    print(f"Warning: Could not initialize recommendation tables: {e}")

# =============================================================================
# OPTIONAL BLUEPRINT REGISTRATION
# (These will be used if you've split your routes into modules.)
# =============================================================================


from libby_backend.blueprints.books.routes import books_bp
app.register_blueprint(books_bp, url_prefix="/api/books")

# Enable recommendations blueprint
from libby_backend.blueprints.recommendations.routes import rec_bp
app.register_blueprint(rec_bp, url_prefix="/api/recommendations")

from libby_backend.blueprints.health.routes import health_bp
app.register_blueprint(health_bp, url_prefix="/api/health")

from libby_backend.blueprints.profile.routes import profile_bp
app.register_blueprint(profile_bp, url_prefix="/api/profile")

from libby_backend.blueprints.utils.routes import utils_bp
app.register_blueprint(utils_bp, url_prefix="/api")

# Register notify prefs endpoints
app.register_blueprint(prefs_bp)

# Scheduler (works if your process stays warm; otherwise use platform cron to hit /api/notify/run-due)
scheduler = BackgroundScheduler(timezone="UTC")
# Job configured with max_instances=1 to prevent overlapping runs
scheduler.add_job(
    send_due_digests_batch,
    "cron",
    hour=2,
    minute=0,
    id="send_due_digests",
    max_instances=1,
)
scheduler.start()

# Ensure scheduler is shut down cleanly when the process exits
atexit.register(lambda: scheduler.shutdown(wait=False))


@app.route("/api/notify/run-due", methods=["POST"])
def run_due_now():
    count = send_due_digests_batch()
    return jsonify({"ok": True, "sent": count})



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
        from libby_backend.database import get_db_connection
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