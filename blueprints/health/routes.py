from flask import Blueprint, jsonify, current_app
from ...extensions import cache

bp = Blueprint("health", __name__, url_prefix="/api")

@bp.get("/health")
def health():
    return jsonify({"status":"healthy","service":"book-recommendation-api","version":"1.0.0"})

@bp.get("/health/detailed")
def detailed():
    try:
        from ...database import get_db_connection
        conn = get_db_connection()
        db_ok = bool(conn)
        if conn: conn.close()

        cache_ok = True
        if current_app.config.get("CACHE_TYPE") == "RedisCache":
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
        return jsonify({"status":"unhealthy","error":str(e)}), 500

@bp.get("/_debug/headers")
def debug_headers():
    from flask import request
    return jsonify({k: v for k, v in request.headers.items()})