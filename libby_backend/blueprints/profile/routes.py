from flask import Blueprint, request, jsonify
from libby_backend.database import get_db_connection

# Register blueprint with URL prefix
profile_bp = Blueprint("profile", __name__, url_prefix="/api/profile")
print("✅ profile.routes.py is loading...")  # Confirm it's imported

# ───────────────────────────────────────────────────────────────
# GET /api/profile/ping → simple health check
# ───────────────────────────────────────────────────────────────
@profile_bp.get("/ping")
def ping():
    return jsonify({"message": "pong"})

# ───────────────────────────────────────────────────────────────
# POST /api/profile/interests → save user interests
# ───────────────────────────────────────────────────────────────
@profile_bp.post("/interests")
def save_interests():
    data = request.get_json()
    print("📩 Received data from frontend:", data)

    user_id = data.get("clerk_user_id")
    interests = data.get("interests", [])

    print("🧑 user_id:", user_id)
    print("📚 interests:", interests)

    if not user_id or not interests:
        print("❌ Missing user_id or interests")
        return jsonify({"error": "Missing user_id or interests"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    print("✅ Connected to DB, clearing old interests...")

    # Create table (dev only)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_interests (
            id SERIAL PRIMARY KEY,
            user_id TEXT NOT NULL,
            genre TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    cursor.execute("DELETE FROM user_interests WHERE user_id = %s;", (user_id,))

    for genre in interests:
        print("   ➤ Inserting genre:", genre)
        cursor.execute("""
            INSERT INTO user_interests (user_id, genre)
            VALUES (%s, %s);
        """, (user_id, genre))

    conn.commit()
    cursor.close()
    conn.close()

    print("✅ Interests saved successfully")
    return jsonify({"message": "Interests saved"}), 200