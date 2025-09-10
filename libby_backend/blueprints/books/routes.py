from flask import Blueprint, jsonify, request
from libby_backend.database import (
    search_books_db, get_book_by_id_db
)
from ...extensions import cache

bp = Blueprint("books", __name__, url_prefix="/api")

@bp.get("/search")
@cache.cached(timeout=180, query_string=True)

def search_books():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify([])
    results = search_books_db(q)
    if results is None:
        return jsonify({"error": "Database connection failed."}), 500
    return jsonify(results)

@bp.get("/books/<int:book_id>")
@cache.cached(timeout=1800)
def get_book(book_id: int):
    book = get_book_by_id_db(book_id)
    if book is None:
        return jsonify({"error": "Database connection failed."}), 500
    if not book:
        return jsonify({"error": "Book not found"}), 404
    return jsonify(dict(book))