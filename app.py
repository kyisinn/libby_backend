# app.py
# FINAL VERSION: This API is fully powered by the PostgreSQL database
# and has been updated to use a consistent response format for all book lists.

# Step 1: Import the necessary libraries
from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2 
import os
from werkzeug.security import generate_password_hash, check_password_hash
from psycopg2.extras import RealDictCursor

# Step 2: Create an instance of the Flask application
app = Flask(__name__)
CORS(app)

# --- PostgreSQL Database Connection ---
def get_db_connection():
    """A function to connect to the PostgreSQL database using the DATABASE_URL."""
    try:
        conn = psycopg2.connect(
            os.environ.get("DATABASE_URL"),
            cursor_factory=RealDictCursor 
        )
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

# --- User Authentication Endpoints ---
# ... (These remain the same)

# --- Book-related Endpoints (Now with consistent responses) ---

@app.route('/api/search', methods=['GET'])
def search_books():
    """Searches for books and returns them in the standard paginated format."""
    query = request.args.get('q', '')
    if not query: return jsonify({"error": "Query required"}), 400
    
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    
    try:
        cursor = conn.cursor()
        search_query = f"%{query}%"
        cursor.execute(
            """
            SELECT b.book_id AS id, b.title, b.cover_image_url AS coverurl, a.first_name || ' ' || a.last_name AS author
            FROM books b JOIN book_authors ba ON b.book_id = ba.book_id JOIN authors a ON ba.author_id = a.author_id
            WHERE b.title ILIKE %s OR (a.first_name || ' ' || a.last_name) ILIKE %s
            """,
            (search_query, search_query)
        )
        results = cursor.fetchall()
        # FIXED: Wrap the results in the standard object format
        return jsonify({
            'books': results,
            'total_books': len(results),
            'page': 1,
            'per_page': len(results)
        })
    finally:
        cursor.close()
        conn.close()

@app.route('/api/recommendations/globally-trending', methods=['GET'])
def get_globally_trending():
    """Gets top books with the highest rating that have a cover, with pagination."""
    page = request.args.get('page', 1, type=int)
    per_page = 20 
    offset = (page - 1) * per_page
    
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    
    try:
        cursor = conn.cursor()
        # UPDATED: This query now orders by rating and ensures a cover image exists.
        cursor.execute(
            """
            SELECT b.book_id AS id, b.title, b.cover_image_url AS coverurl, b.rating, a.first_name || ' ' || a.last_name AS author
            FROM books b JOIN book_authors ba ON b.book_id = ba.book_id JOIN authors a ON ba.author_id = a.author_id
            WHERE b.cover_image_url IS NOT NULL AND b.cover_image_url <> '' AND b.rating IS NOT NULL
            ORDER BY b.rating DESC
            LIMIT %s OFFSET %s
            """,
            (per_page, offset)
        )
        results = cursor.fetchall()
        
        # UPDATED: The count query must also match the new WHERE clause.
        cursor.execute("SELECT COUNT(*) FROM books WHERE cover_image_url IS NOT NULL AND cover_image_url <> '' AND rating IS NOT NULL")
        total_books = cursor.fetchone()['count']
        
        return jsonify({
            'books': results,
            'total_books': total_books,
            'page': page,
            'per_page': per_page
        })
    finally:
        cursor.close()
        conn.close()

@app.route('/api/recommendations/by-major', methods=['GET'])
def get_by_major():
    """Gets books by major and returns them in the standard paginated format."""
    major = request.args.get('major', 'Computer Science', type=str)
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    
    try:
        cursor = conn.cursor()
        search_query = f"%{major}%"
        cursor.execute(
            """
            SELECT b.book_id AS id, b.title, b.cover_image_url AS coverurl, a.first_name || ' ' || a.last_name AS author
            FROM books b JOIN book_authors ba ON b.book_id = ba.book_id JOIN authors a ON ba.author_id = a.author_id
            WHERE b.genre ILIKE %s
            ORDER BY b.rating DESC NULLS LAST LIMIT 10
            """,
            (search_query,)
        )
        results = cursor.fetchall()
        # FIXED: Wrap the results in the standard object format
        return jsonify({
            'books': results,
            'total_books': len(results),
            'page': 1,
            'per_page': 10
        })
    finally:
        cursor.close()
        conn.close()

# ... (other routes like /api/books/<id> remain the same) ...

# Step 5: Run the Flask Application
if __name__ == '__main__':
    app.run(debug=True)
