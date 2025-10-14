from flask import Blueprint, jsonify
from db import get_db_connection

admin_bp = Blueprint("admin_bp", __name__)

@admin_bp.route("/api/admin/overview", methods=["GET"])
def admin_overview():
    conn = get_db_connection()
    cursor = conn.cursor()

    # --- Total users from clerk_user_id
    cursor.execute("SELECT COUNT(DISTINCT clerk_user_id) FROM recommendations;")
    total_users = cursor.fetchone()[0]

    # --- New users this week (based on recommendations created recently)
    cursor.execute("""
        SELECT COUNT(DISTINCT clerk_user_id)
        FROM recommendations
        WHERE create_at >= NOW() - INTERVAL '7 days';
    """)
    new_signups_week = cursor.fetchone()[0]

    # --- Active recommendations (last 30 days)
    cursor.execute("""
        SELECT COUNT(*)
        FROM recommendations
        WHERE create_at >= NOW() - INTERVAL '30 days';
    """)
    active_recommendations = cursor.fetchone()[0]

    # --- Total books
    cursor.execute("SELECT COUNT(*) FROM books;")
    total_books = cursor.fetchone()[0]

    conn.close()

    return jsonify({
        "total_users": total_users,
        "new_signups_week": new_signups_week,
        "active_recommendations": active_recommendations,
        "total_books": total_books
    })