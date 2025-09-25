from flask import Blueprint, request, jsonify
from libby_backend.database import get_db_connection

clerk_bp = Blueprint("clerk", __name__, url_prefix="/api/clerk")

@clerk_bp.post("/webhook")
def clerk_webhook():
    event = request.get_json(force=True)
    event_type = event.get("type")

    if event_type in ["session.created", "user.signed_in"]:
        clerk_user_id = event["data"]["user_id"]

        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO user_logins (clerk_user_id) VALUES (%s);",
                    (clerk_user_id,)
                )
                conn.commit()
        except Exception as e:
            print("clerk_webhook error:", e)
            return jsonify({"error": "DB insert failed"}), 500
        finally:
            conn.close()

    return jsonify({"success": True})