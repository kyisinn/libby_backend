# libby_backend/cache.py
import os
from flask_caching import Cache

def init_cache(app):
    """
    Configure and initialize Flask-Caching.
    Uses Redis if REDIS_URL is present; falls back to in-memory SimpleCache.
    """
    redis_url = os.getenv("REDIS_URL")
    default_timeout = int(os.getenv("CACHE_DEFAULT_TIMEOUT", "300"))

    if redis_url:
        app.config.update(
            CACHE_TYPE="RedisCache",
            CACHE_REDIS_URL=redis_url,
            CACHE_DEFAULT_TIMEOUT=default_timeout,
            CACHE_KEY_PREFIX="libby:",
        )
    else:
        app.config.update(
            CACHE_TYPE="SimpleCache",
            CACHE_DEFAULT_TIMEOUT=default_timeout,
        )

    cache = Cache(app)
    # Optional: quick log
    print(f"[cache] backend={app.config.get('CACHE_TYPE')} timeout={default_timeout}s")
    return cache