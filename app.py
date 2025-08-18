# =============================================================================
# FLASK BOOK RECOMMENDATION API - MAIN BACKEND (NO CACHE)
# =============================================================================
# Main Flask application that handles all API endpoints and routing

from flask import Flask, jsonify, request, g
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
import os, datetime, jwt
from functools import wraps
from cache import init_cache

# Import our custom modules
from database import (
    search_books_db, 
    get_trending_books_db, 
    get_books_by_major_db, 
    get_book_by_id_db,
    get_similar_books_details
)

# =============================================================================
# APPLICATION SETUP
# =============================================================================

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}}, supports_credentials=False)
cache = init_cache(app)

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-me")
JWT_EXPIRES_DAYS = int(os.getenv("JWT_EXPIRES_DAYS", "7"))

def create_jwt(user_id: int, email: str) -> str:
    payload = {
        "sub": user_id,
        "email": email,
        "iat": datetime.datetime.utcnow(),
        "exp": datetime.datetime.utcnow() + datetime.timedelta(days=JWT_EXPIRES_DAYS)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")

def auth_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return jsonify({"error": "Missing or invalid Authorization header"}), 401
        token = auth_header.split(" ", 1)[1].strip()
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
            g.user_id = payload.get("sub")
            g.email = payload.get("email")
            if not g.user_id:
                return jsonify({"error": "Invalid token"}), 401
        except jwt.ExpiredSignatureError:
            return jsonify({"error": "Token expired"}), 401
        except Exception as e:
            print("JWT decode error:", e)
            return jsonify({"error": "Invalid token"}), 401
        return fn(*args, **kwargs)
    return wrapper

# =============================================================================
# USER AUTHENTICATION ENDPOINTS
# =============================================================================
from database import get_user_by_email, create_user, get_user_by_id

@app.route("/api/auth/signup", methods=["POST"])
def auth_signup():
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip()
    password = data.get("password") or ""
    first_name = (data.get("first_name") or "").strip() or None
    last_name  = (data.get("last_name") or "").strip() or None
    phone      = (data.get("phone") or "").strip() or None

    if not email or not password:
        return jsonify({"error": "Email and password are required"}), 400
    if len(password) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400

    # Check existing user
    existing = get_user_by_email(email)
    if existing:
        return jsonify({"error": "Email already in use"}), 409

    pwd_hash = generate_password_hash(password)
    created = create_user(email, pwd_hash, first_name, last_name, phone)
    if created is None:
        return jsonify({"error": "Database error creating user"}), 500
    if isinstance(created, dict) and created.get("_duplicate"):
        return jsonify({"error": "Email already in use"}), 409

    token = create_jwt(created["user_id"], created["email"])
    return jsonify({
        "token": token,
        "user": {
            "user_id": created["user_id"],
            "email": created["email"],
            "first_name": created.get("first_name"),
            "last_name": created.get("last_name"),
            "phone": created.get("phone"),
            "membership_type": created.get("membership_type"),
            "is_active": created.get("is_active"),
            "created_at": created["created_at"].isoformat() if created.get("created_at") else None
        }
    }), 201

@app.route("/api/auth/login", methods=["POST"])
def auth_login():
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip()
    password = data.get("password") or ""
    if not email or not password:
        return jsonify({"error": "Email and password are required"}), 400

    user = get_user_by_email(email)
    if not user or not check_password_hash(user["password_hash"], password):
        return jsonify({"error": "Invalid credentials"}), 401

    token = create_jwt(user["user_id"], user["email"])
    return jsonify({
        "token": token,
        "user": {
            "user_id": user["user_id"],
            "email": user["email"],
            "first_name": user.get("first_name"),
            "last_name": user.get("last_name"),
            "phone": user.get("phone"),
            "membership_type": user.get("membership_type"),
            "is_active": user.get("is_active"),
            "created_at": user.get("created_at").isoformat() if user.get("created_at") else None
        }
    }), 200

@app.route("/api/auth/me", methods=["GET"])
@auth_required
def auth_me():
    user = get_user_by_id(g.user_id)
    if not user:
        return jsonify({"error": "User not found"}), 404
    return jsonify({
        "user_id": user["user_id"],
        "email": user["email"],
        "first_name": user.get("first_name"),
        "last_name": user.get("last_name"),
        "phone": user.get("phone"),
        "membership_type": user.get("membership_type"),
        "is_active": user.get("is_active"),
        "created_at": user.get("created_at").isoformat() if user.get("created_at") else None
    }), 200

# =============================================================================
# BOOK SEARCH ENDPOINTS
# =============================================================================

@app.route('/api/search', methods=['GET'])
@cache.cached(timeout=180, query_string=True)

def search_books():
    """Searches for books and returns a simple list."""
    query = request.args.get('q', '').strip()
    if not query: 
        return jsonify([])
    
    # Get from database directly
    results = search_books_db(query)
    if results is None:
        return jsonify({"error": "Database connection failed."}), 500
    
    return jsonify(results)

# =============================================================================
# BOOK RECOMMENDATION ENDPOINTS
# =============================================================================

@app.route('/api/recommendations/globally-trending', methods=['GET'])
@cache.cached(timeout=600, query_string=True)

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
@cache.cached(timeout=600, query_string=True)

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
    """Gets content-based recommendations (simplified without cache)."""
    try:
        # For now, return similar books based on the same genre/category
        # This is a simplified approach - you could implement basic similarity logic here
        # or remove this endpoint until you implement proper similarity calculations
        
        # Get the target book first
        target_book = get_book_by_id_db(book_id)
        if not target_book:
            return jsonify({"error": "Book not found."}), 404
        
        # For demonstration, return an empty list
        # You can implement basic similarity logic here later
        return jsonify([])

    except Exception as e:
        print(f"Error in get_similar_books: {e}")
        return jsonify({"error": "An internal error occurred."}), 500

# =============================================================================
# INDIVIDUAL BOOK ENDPOINTS
# =============================================================================

@app.route('/api/books/<int:book_id>', methods=['GET'])
@cache.cached(timeout=1800)

def get_book_by_id(book_id):
    """Gets all details for a single book by its ID."""
    book = get_book_by_id_db(book_id)
    
    if book is None:
        return jsonify({"error": "Database connection failed."}), 500
    
    if book:
        return jsonify(dict(book))
    else:
        return jsonify({"error": "Book not found"}), 404

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
    try:
        from database import get_db_connection
        conn = get_db_connection()
        db_ok = bool(conn)
        if conn: conn.close()

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