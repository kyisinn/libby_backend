from flask import Blueprint, request, jsonify
from libby_backend.database import get_db_connection

# Create the blueprint
profile_bp = Blueprint("profile", __name__)
print("✅ profile.routes.py is loading...")  # Debug to confirm module import

@profile_bp.route("/interests", methods=["POST"])
def save_interests():
    try:
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

        # Create table if it doesn't exist (dev only)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_interests (
                id SERIAL PRIMARY KEY,
                user_id TEXT NOT NULL,
                genre TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Delete previous interests for the user
        cursor.execute("DELETE FROM user_interests WHERE user_id = %s;", (user_id,))

        # Insert new interests
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

    except Exception as e:
        print("🔥 Error saving interests:", e)
        return jsonify({"error": str(e)}), 500

@profile_bp.route("/ping", methods=["GET"])
def ping():
    return jsonify({"message": "pong"}), 200