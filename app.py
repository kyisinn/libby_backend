# app.py
# FINAL VERSION: This API is fully powered by the PostgreSQL database
# and has been updated to return simple lists for easier debugging.

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

# --- Book-related Endpoints (Now returning simple lists) ---

@app.route('/api/search', methods=['GET'])
def search_books():
    """Searches for books and returns a simple list."""
    query = request.args.get('q', '')
    if not query: return jsonify([]) # Return empty list if no query
    
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    
    try:
        cursor = conn.cursor()
        search_query = f"%{query}%"
        cursor.execute(
            """
            SELECT
                b.book_id AS id,
                b.title,
                b.cover_image_url AS coverurl,
                COALESCE(a.first_name || ' ' || a.last_name, 'Unknown Author') AS author
            FROM books b
            LEFT JOIN book_authors ba ON b.book_id = ba.book_id
            LEFT JOIN authors a ON ba.author_id = a.author_id
            WHERE b.title ILIKE %s OR (a.first_name || ' ' || a.last_name) ILIKE %s
            """,
            (search_query, search_query)
        )
        results = cursor.fetchall()
        return jsonify(results) # Return a direct list
    finally:
        cursor.close()
        conn.close()

@app.route('/api/recommendations/globally-trending', methods=['GET'])
def get_globally_trending():
    """Gets top books published in the current month that have a cover."""
    page = request.args.get('page', 1, type=int)
    per_page = 20 
    offset = (page - 1) * per_page
    
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    
    try:
        cursor = conn.cursor()
        # --- THIS IS THE UPDATED QUERY ---
        # This query now filters for books published in the last five years.
        cursor.execute(
            """
            SELECT
                b.book_id AS id,
                b.title,
                b.cover_image_url AS coverurl,
                b.rating,
                COALESCE(a.first_name || ' ' || a.last_name, 'Unknown Author') AS author
            FROM books b
            LEFT JOIN
                book_authors ba ON b.book_id = ba.book_id
            LEFT JOIN
                authors a ON ba.author_id = a.author_id
            WHERE
                b.cover_image_url IS NOT NULL AND b.cover_image_url <> ''
                AND b.publication_date >= CURRENT_DATE - INTERVAL '5 years'
            ORDER BY
                b.publication_date DESC
            LIMIT %s OFFSET %s
            """,
            (per_page, offset)
        )
        results = cursor.fetchall()
        
        # The count query must also match the new WHERE clause.
        cursor.execute("""
            SELECT COUNT(*) FROM books 
            WHERE cover_image_url IS NOT NULL AND cover_image_url <> ''
            AND publication_date >= CURRENT_DATE - INTERVAL '5 years'
        """)
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
    """Gets books by major and returns a simple list."""
    major = request.args.get('major', 'Computer Science', type=str)
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    
    try:
        cursor = conn.cursor()
        search_query = f"%{major}%"
        cursor.execute(
            """
            SELECT
                b.book_id AS id,
                b.title,
                b.cover_image_url AS coverurl,
                COALESCE(a.first_name || ' ' || a.last_name, 'Unknown Author') AS author
            FROM books b
            LEFT JOIN book_authors ba ON b.book_id = ba.book_id
            LEFT JOIN authors a ON ba.author_id = a.author_id
            WHERE b.genre ILIKE %s
            ORDER BY b.rating DESC NULLS LAST
            LIMIT 10
            """,
            (search_query,)
        )
        results = cursor.fetchall()
        return jsonify(results) # Return a direct list
    finally:
        cursor.close()
        conn.close()

@app.route('/api/books/<int:book_id>', methods=['GET'])
def get_book_by_id(book_id):
    """Gets all details for a single book by its ID."""
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500

    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                b.*,
                COALESCE(a.first_name || ' ' || a.last_name, 'Unknown Author') AS author
            FROM books b
            LEFT JOIN book_authors ba ON b.book_id = ba.book_id
            LEFT JOIN authors a ON ba.author_id = a.author_id
            WHERE b.book_id = %s
            """,
            (book_id,)
        )
        book = cursor.fetchone()

        if book:
            book['id'] = book.pop('book_id')
            book['coverurl'] = book.pop('cover_image_url')
            return jsonify(book)
        else:
            return jsonify({"error": "Book not found"}), 404
    finally:
        cursor.close()
        conn.close()

# Step 5: Run the Flask Application
if __name__ == '__main__':
    app.run(debug=True)
