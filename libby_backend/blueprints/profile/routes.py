from flask import Blueprint, request, jsonify
from flask_cors import cross_origin, CORS
from libby_backend.database import get_db_connection, save_user_interests_db


# Register blueprint with URL prefix
profile_bp = Blueprint("profile", __name__, url_prefix="/api/profile")
print("✅ profile.routes.py is loading...")  # Confirm it's imported

# Apply per-blueprint CORS so we don't need to decorate every route manually
CORS(profile_bp, origins=["http://localhost:3000", "http://127.0.0.1:3000", "https://libby-bot.vercel.app"],
    supports_credentials=True, methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"]) 

from flask import Blueprint, request, jsonify


# Import centralized user ID resolver
from libby_backend.utils.user_resolver import resolve_user_id, resolve_user_id_from_request

# ───────────────────────────────────────────────────────────────
# GET /api/profile/ping → simple health check
# ───────────────────────────────────────────────────────────────
@profile_bp.get("/ping")
@cross_origin(origins=["http://localhost:3000", "https://libby-bot.vercel.app"])
def ping():
    return jsonify({"message": "pong"})

# ───────────────────────────────────────────────────────────────
# POST /api/profile/interests → save user interests
# ───────────────────────────────────────────────────────────────
@profile_bp.post("/interests")
@cross_origin(origins=["http://localhost:3000", "https://libby-bot.vercel.app"])
def save_interests():
    data = request.get_json()
    print("📩 Received data from frontend:", data)

    clerk_user_id = data.get("clerk_user_id")
    interests = data.get("interests", [])

    print("🧑 clerk_user_id:", clerk_user_id)
    print("📚 interests:", interests)

    if not clerk_user_id or not interests:
        print("❌ Missing clerk_user_id or interests")
        return jsonify({"error": "Missing clerk_user_id or interests"}), 400

    # Convert Clerk ID to integer (same format as user_interactions table)
    user_id_int = resolve_user_id(clerk_user_id)
    print(f"🔗 Converted {clerk_user_id} -> {user_id_int}")

    success = save_user_interests_db(user_id_int, clerk_user_id, interests)
    if success:
        print("✅ Interests saved successfully")
        return jsonify({"success": True, "interests": interests}), 200
    else:
        print("❌ DB error saving interests")
        return jsonify({"error": "DB error saving interests"}), 500

# ───────────────────────────────────────────────────────────────
# GET /api/profile/interests → fetch user interests
# ───────────────────────────────────────────────────────────────
@profile_bp.get("/interests")
@cross_origin(origins=["http://localhost:3000", "https://libby-bot.vercel.app"])
def get_interests():
    clerk_user_id = request.args.get("user_id")
    print(f"🔍 Fetching interests for Clerk ID: {clerk_user_id}")

    if not clerk_user_id:
        return jsonify({"error": "Missing user_id"}), 400

    # Convert Clerk ID to integer (same format as user_interactions table)
    user_id_int = resolve_user_id(clerk_user_id)
    print(f"🔗 Converted {clerk_user_id} -> {user_id_int}")

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT genre FROM user_interests
        WHERE user_id = %s
        ORDER BY created_at ASC;
    """, (user_id_int,))
    genres = [row["genre"] for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return jsonify({"genres": genres})