# app.py
# FINAL VERSION: This API is fully powered by the PostgreSQL database
# and has been updated to use the new, relational table structure.

# Step 1: Import the necessary libraries
from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2 
import os
from werkzeug.security import generate_password_hash, check_password_hash
from psycopg2.extras import RealDictCursor # To get results as dictionaries

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
@app.route('/api/register', methods=['POST'])
def register_user():
    """Registers a new user in the 'users' table."""
    data = request.get_json()
    if not data or not data.get('email') or not data.get('password') or not data.get('firstName') or not data.get('lastName'):
        return jsonify({"error": "First name, last name, email, and password are required."}), 400
    
    email = data['email']
    first_name = data['firstName']
    last_name = data['lastName']
    password_hash = generate_password_hash(data['password'])
    
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    
    try:
        cursor = conn.cursor()
        # Assumes a 'password_hash' column has been added to your users table.
        cursor.execute(
            "INSERT INTO users (first_name, last_name, email, password_hash) VALUES (%s, %s, %s, %s)",
            (first_name, last_name, email, password_hash)
        )
        conn.commit()
    except psycopg2.IntegrityError:
        return jsonify({"error": "This email address is already registered."}), 409
    finally:
        cursor.close()
        conn.close()
        
    return jsonify({"message": "User registered successfully!"}), 201

@app.route('/api/login', methods=['POST'])
def login_user():
    """Logs in a user by checking their credentials against the 'users' table."""
    data = request.get_json()
    if not data or not data.get('email') or not data.get('password'):
        return jsonify({"error": "Email and password are required."}), 400
    
    email = data['email']
    password = data['password']
    
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users WHERE email = %s", (email,))
        user = cursor.fetchone()
        
        # Assumes a 'password_hash' column exists.
        if user and check_password_hash(user["password_hash"], password):
            return jsonify({"message": "Login successful!", "user_id": user["user_id"]}), 200
        else:
            return jsonify({"error": "Invalid email or password."}), 401
    finally:
        cursor.close()
        conn.close()

# --- Book-related Endpoints ---

@app.route('/api/search', methods=['GET'])
def search_books():
    """Searches for books by title or author name."""
    query = request.args.get('q', '')
    if not query: return jsonify({"error": "Query required"}), 400
    
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
                a.first_name || ' ' || a.last_name AS author
            FROM
                books b
            JOIN
                book_authors ba ON b.book_id = ba.book_id
            JOIN
                authors a ON ba.author_id = a.author_id
            WHERE
                b.title ILIKE %s OR (a.first_name || ' ' || a.last_name) ILIKE %s
            """,
            (search_query, search_query)
        )
        results = cursor.fetchall()
        return jsonify(results)
    finally:
        cursor.close()
        conn.close()

@app.route('/api/recommendations/globally-trending', methods=['GET'])
def get_globally_trending():
    """Gets the 10 highest-rated books that have a cover image."""
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    
    try:
        cursor = conn.cursor()
        # UPDATED: Aliased cover_image_url to coverurl to match the frontend's expectation.
        cursor.execute(
            """
            SELECT
                b.book_id AS id,
                b.title,
                b.cover_image_url AS coverurl,
                b.rating,
                a.first_name || ' ' || a.last_name AS author
            FROM
                books b
            JOIN
                book_authors ba ON b.book_id = ba.book_id
            JOIN
                authors a ON ba.author_id = a.author_id
            WHERE
                b.cover_image_url IS NOT NULL AND b.cover_image_url <> '' AND b.rating IS NOT NULL
            ORDER BY
                b.rating DESC
            LIMIT 10
            """
        )
        results = cursor.fetchall()
        return jsonify(results)
    finally:
        cursor.close()
        conn.close()

@app.route('/api/recommendations/based-on-book/<int:book_id>', methods=['GET'])
def recommend_based_on_book(book_id):
    """Recommends other books from the same category."""
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    
    try:
        cursor = conn.cursor()
        # Assumes a 'book_categories' join table exists (book_id, category_id).
        cursor.execute("SELECT category_id FROM book_categories WHERE book_id = %s", (book_id,))
        categories = cursor.fetchall()
        
        if not categories:
            return jsonify({"error": "Source book has no categories to base recommendations on."}), 404
        
        category_ids = [cat['category_id'] for cat in categories]
        
        cursor.execute(
            """
            SELECT DISTINCT
                b.book_id AS id,
                b.title,
                b.cover_image_url AS coverurl,
                a.first_name || ' ' || a.last_name AS author
            FROM books b
            JOIN book_authors ba ON b.book_id = ba.book_id
            JOIN authors a ON ba.author_id = a.author_id
            JOIN book_categories bc ON b.book_id = bc.book_id
            WHERE bc.category_id = ANY(%s) AND b.book_id != %s
            ORDER BY RANDOM() 
            LIMIT 10
            """,
            (category_ids, book_id)
        )
        recommendations = cursor.fetchall()
        return jsonify(recommendations)
    finally:
        cursor.close()
        conn.close()

# Step 5: Run the Flask Application
if __name__ == '__main__':
    app.run(debug=True)
