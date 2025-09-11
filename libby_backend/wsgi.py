# libby_backend/wsgi.py
from libby_backend.app import app

if __name__ == "__main__":
    app.run()