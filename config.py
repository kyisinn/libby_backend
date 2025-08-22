import os

class Config:
    # Core
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-change-me")
    JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-me")
    JWT_EXPIRES_DAYS = int(os.getenv("JWT_EXPIRES_DAYS", "7"))
    FLASK_ENV = os.getenv("FLASK_ENV", "production")

    # CORS
    FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "https://libby-bot.vercel.app")
    CORS_RESOURCES = {r"/api/*": {"origins": [FRONTEND_ORIGIN]}}
    CORS_SUPPORTS_CREDENTIALS = True
    CORS_ALLOW_HEADERS = [
        "*", "Authorization", "authorization", "Http-Authorization",
        "HTTP_AUTHORIZATION", "X-Authorization", "X-Forwarded-Authorization",
        "Content-Type",
    ]
    CORS_EXPOSE_HEADERS = ["Content-Type"]

    # Cache (flask-caching)
    CACHE_TYPE = os.getenv("CACHE_TYPE", "SimpleCache")  # "RedisCache" in Railway
    CACHE_DEFAULT_TIMEOUT = int(os.getenv("CACHE_DEFAULT_TIMEOUT", "300"))
    CACHE_REDIS_URL = os.getenv("REDIS_URL")  # when using Redis