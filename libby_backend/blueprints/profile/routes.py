from flask import Blueprint, request, jsonify
from libby_backend.database import get_db_connection
 
profile_bp = Blueprint("profile", __name__)
 
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
            return jsonify({"error": "Missing user_id or interests"}), 400
 
        conn = get_db_connection()
        cursor = conn.cursor()
        print("✅ Connected to DB, clearing old interests...")
 
        # Create table if it doesn't exist (for dev)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_interests (
                id SERIAL PRIMARY KEY,
                user_id TEXT NOT NULL,
                genre TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
 
        # Remove existing interests for this user
        cursor.execute("DELETE FROM user_interests WHERE user_id = %s;", (user_id,))
  
        # Insert each selected genre
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