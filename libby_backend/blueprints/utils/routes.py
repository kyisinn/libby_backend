from flask import Blueprint, jsonify
from libby_backend.cache import cache
import logging

utils_bp = Blueprint("utils", __name__)
logger = logging.getLogger(__name__)

@utils_bp.route("/admin/clear_cache", methods=["POST"])
def clear_cache():
    """Clear the application cache"""
    try:
        cache.clear()
        logger.info("Cache cleared successfully")
        return jsonify({
            "success": True, 
            "message": "Cache cleared successfully"
        }), 200
    except Exception as e:
        logger.error(f"Error clearing cache: {e}")
        return jsonify({
            "success": False, 
            "error": str(e)
        }), 500

@utils_bp.route("/admin/clear_cache", methods=["GET"])
def clear_cache_get():
    """Clear the application cache (GET method for easy testing)"""
    try:
        cache.clear()
        logger.info("Cache cleared successfully via GET")
        return jsonify({
            "status": "cache cleared",
            "success": True, 
            "message": "Cache cleared successfully"
        }), 200
    except Exception as e:
        logger.error(f"Error clearing cache: {e}")
        return jsonify({
            "status": "error",
            "success": False, 
            "error": str(e)
        }), 500

@utils_bp.route("/admin/health", methods=["GET"])
def admin_health():
    """Administrative health check with more details"""
    try:
        # Test database connection
        db_status = "unknown"
        try:
            from libby_backend.database import get_db_connection
            conn = get_db_connection()
            if conn:
                conn.close()
                db_status = "connected"
            else:
                db_status = "failed"
        except Exception as e:
            db_status = f"error: {str(e)}"
        
        # Test cache
        cache_status = "unknown"
        try:
            cache.set("health_test", "ok", timeout=5)
            if cache.get("health_test") == "ok":
                cache_status = "connected"
            else:
                cache_status = "failed"
        except Exception as e:
            cache_status = f"error: {str(e)}"
        
        return jsonify({
            "success": True,
            "database": db_status,
            "cache": cache_status,
            "message": "Administrative health check complete"
        }), 200
        
    except Exception as e:
        logger.error(f"Error in admin health check: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@utils_bp.route("/admin/cache_status", methods=["GET"])
def cache_status():
    """Check cache status and test cache functionality"""
    try:
        # Test cache write/read
        test_key = "cache_test"
        test_value = "working"
        
        cache.set(test_key, test_value, timeout=10)
        retrieved_value = cache.get(test_key)
        
        cache_working = retrieved_value == test_value
        
        return jsonify({
            "success": True,
            "cache_working": cache_working,
            "test_result": f"Set '{test_value}', Got '{retrieved_value}'",
            "message": "Cache is working" if cache_working else "Cache test failed"
        }), 200
        
    except Exception as e:
        logger.error(f"Error checking cache status: {e}")
        return jsonify({
            "success": False,
            "cache_working": False,
            "error": str(e)
        }), 500
