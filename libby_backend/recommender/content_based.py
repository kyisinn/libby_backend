"""
content_based.py
----------------
Content-Based Filtering module for Libby-Bot.
Uses TF-IDF + cosine similarity on book metadata
(title, description, genre) stored in PostgreSQL.
"""

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from libby_backend.database import get_db_connection

# ---------------------------
# 1. Database connection
# ---------------------------

# ---------------------------
# 2. Fetch books from DB
# ---------------------------

def fetch_books(limit=None):
    conn = get_db_connection()
    query = """
        SELECT book_id, isbn, title, description, genre, author, rating
        FROM books
        WHERE description IS NOT NULL AND description <> ''
    """
    if limit:
        query += f" LIMIT {limit}"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


# ---------------------------
# 3. Create combined text field
# ---------------------------

def build_text_features(df):
    # Fill missing values with blanks
    for col in ['title', 'description', 'genre', 'author']:
        df[col] = df[col].fillna('')
    # Combine fields into one text column
    df['combined'] = (
        df['title'] + ' ' +
        df['author'] + ' ' +
        df['genre'] + ' ' +
        df['description']
    )
    return df


# ---------------------------
# 4. Build TF-IDF and Similarity Matrix
# ---------------------------

def build_tfidf_matrix(df):
    tfidf = TfidfVectorizer(stop_words='english', max_features=5000)
    tfidf_matrix = tfidf.fit_transform(df['combined'])
    similarity_matrix = cosine_similarity(tfidf_matrix)
    return similarity_matrix


# ---------------------------
# 5. Recommend similar books
# ---------------------------

def get_similar_books(book_id, top_n=10):
    df = fetch_books()
    df = build_text_features(df)
    sim_matrix = build_tfidf_matrix(df)

    # Map book_id to index
    if book_id not in df['book_id'].values:
        raise ValueError("Book ID not found in dataset.")

    idx = df.index[df['book_id'] == book_id][0]

    # Get similarity scores
    sim_scores = list(enumerate(sim_matrix[idx]))
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
    top_books = sim_scores[1:top_n+1]  # skip self

    # Return top N recommendations
    recommendations = df.iloc[[i[0] for i in top_books]][
        ['book_id', 'title', 'author', 'genre', 'rating']
    ].copy()
    recommendations['similarity'] = [i[1] for i in top_books]
    return recommendations


# ---------------------------
# 6. Example run
# ---------------------------

if __name__ == "__main__":
    # Example: Get similar books for book_id = 101
    results = get_similar_books(book_id=101, top_n=5)
    print("Top 5 similar books:")
    print(results)
