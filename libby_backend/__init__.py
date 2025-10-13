import os
from flask import Flask
from flask_cors import CORS
from libby_backend.config import Config
from libby_backend.cache import init_cache
from libby_backend.extensions import cache

# Load .env only in local/dev
try:
    if os.getenv("FLASK_ENV", "production") != "production":
        from dotenv import load_dotenv
        load_dotenv()
except Exception:
    pass


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    # CORS
    CORS(
        app,
        resources=app.config["CORS_RESOURCES"],
        supports_credentials=app.config["CORS_SUPPORTS_CREDENTIALS"],
        allow_headers=app.config["CORS_ALLOW_HEADERS"],
        expose_headers=app.config["CORS_EXPOSE_HEADERS"],
    )

    # Cache
    init_cache(app)

    # Blueprints
    from libby_backend.blueprints.books.routes import bp as books_bp
    from libby_backend.blueprints.health.routes import bp as health_bp

    app.register_blueprint(books_bp)
    app.register_blueprint(health_bp)

    return app