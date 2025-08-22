from flask import Flask
from flask_cors import CORS
from .config import Config
from .cache import init_cache
from .extensions import cache

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
    from .blueprints.auth.routes import bp as auth_bp
    from .blueprints.books.routes import bp as books_bp
    from .blueprints.health.routes import bp as health_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(books_bp)
    app.register_blueprint(health_bp)

    return app