import os

class Config:
    # --- Core ---
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-change-me")
    JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-me")
    JWT_EXPIRES_DAYS = int(os.getenv("JWT_EXPIRES_DAYS", "7"))
    FLASK_ENV = os.getenv("FLASK_ENV", "production")

    # --- CORS ---
    FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "https://libby-bot.vercel.app")
    EXTRA_ORIGINS = os.getenv("EXTRA_ORIGINS", "http://localhost:3000").split(",")

    CORS_RESOURCES = {
        r"/api/*": {
            "origins": [FRONTEND_ORIGIN, *[o.strip() for o in EXTRA_ORIGINS if o.strip()]],
            "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        }
    }
    CORS_SUPPORTS_CREDENTIALS = True
    # Be permissive on headers so Authorization is never stripped
    CORS_ALLOW_HEADERS = [
        "*",
        "Authorization", "authorization",
        "Http-Authorization", "HTTP_AUTHORIZATION",
        "X-Authorization", "X-Forwarded-Authorization",
        "Content-Type",
    ]
    CORS_EXPOSE_HEADERS = ["Content-Type"]

    # --- Cache (Flask-Caching) ---
    # Use RedisCache in Railway (set REDIS_URL). Falls back to SimpleCache locally.
    CACHE_TYPE = os.getenv("CACHE_TYPE", "RedisCache" if os.getenv("REDIS_URL") else "SimpleCache")
    CACHE_DEFAULT_TIMEOUT = int(os.getenv("CACHE_DEFAULT_TIMEOUT", "300"))
    CACHE_REDIS_URL = os.getenv("REDIS_URL")
    CACHE_KEY_PREFIX = os.getenv("CACHE_KEY_PREFIX", "libby:")