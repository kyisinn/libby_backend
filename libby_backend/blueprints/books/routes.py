from flask import Blueprint, jsonify, request
from libby_backend.database import (
        search_books_db, get_trending_books_db, get_books_by_major_db, get_book_by_id_db
)
from ...extensions import cache

books_bp = Blueprint("books", __name__, url_prefix="/api/books")

@books_bp.get("/search")
@cache.cached(timeout=30, query_string=True)  # Reduced from 180 to 30 seconds

def search_books():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify([])
    results = search_books_db(q)
    if results is None:
        return jsonify({"error": "Database connection failed."}), 500
    return jsonify(results)


@books_bp.get("/recommendations/globally-trending")
@cache.cached(timeout=60, query_string=True)  # Reduced from 600 to 60 seconds
def globally_trending():
    period = request.args.get("period", "5years", type=str)
    page   = request.args.get("page", 1, type=int)
    per_page = 20
    result = get_trending_books_db(period, page, per_page)
    if result is None:
        return jsonify({"error": "Database connection failed."}), 500
    return jsonify({
        "books": result["books"],
        "total_books": result["total_books"],
        "page": page,
        "per_page": per_page,
    })

@books_bp.get("/recommendations/by-major")
@cache.cached(timeout=60, query_string=True)  # Reduced from 600 to 60 seconds
def by_major():
    major = request.args.get("major", "Computer Science", type=str)
    page  = request.args.get("page", 1, type=int)
    per_page = 15
    result = get_books_by_major_db(major, page, per_page)
    if result is None:
        return jsonify({"error": "Database connection failed."}), 500
    return jsonify({
        "books": result["books"],
        "total_books": result["total_books"],
        "page": page,
        "per_page": per_page,
    })

@books_bp.get("/recommendations/similar-to/<int:book_id>")
def similar_to(book_id: int):
    # placeholder (kept minimal as in your code)
    target = get_book_by_id_db(book_id)
    if not target:
        return jsonify({"error": "Book not found."}), 404
    return jsonify([])


@books_bp.get("/books/<int:book_id>")
@cache.cached(timeout=1800)
def get_book(book_id: int):
    book = get_book_by_id_db(book_id)
    if book is None:
        return jsonify({"error": "Database connection failed."}), 500
    if not book:
        return jsonify({"error": "Book not found"}), 404
    return jsonify(dict(book))