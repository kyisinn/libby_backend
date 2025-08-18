# wsgi.py (repo root)
from .app import app  # adjust if your inner package is named differently

# optional: for local run `python wsgi.py`
if __name__ == "__main__":
    app.run()