"""
One-time import script with staging table.
This version is production-safe for Railway deployment.
"""

import psycopg2
import requests
import io
import time

# === CONFIG ===
DB_URL = "postgresql://postgres:pAflkfysMwUFGUPGzcbLBfUvoVJJjazQ@postgres.railway.internal:5432/railway"
CSV_URL = "https://raw.githubusercontent.com/kyisinn/Data/main/AU_Library_Books_Finalv2.csv"

def import_books():
    print("📦 Starting staged import...")
    start = time.time()
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    # Check if main table already has data
    cur.execute("SELECT COUNT(*) FROM books;")
    count = cur.fetchone()[0]
    if count > 0:
        print(f"⚠️ Books table already has {count} rows — skipping import.")
        conn.close()
        return

    # Step 1. Create temporary staging table
    print("🧱 Creating temporary books_stage table...")
    cur.execute("""
        DROP TABLE IF EXISTS books_stage;
        CREATE TEMP TABLE books_stage (
            isbn TEXT,
            title TEXT,
            author TEXT,
            description TEXT,
            publication_date TEXT,
            cover_image_url TEXT,
            genre TEXT,
            rating TEXT
        );
    """)
    conn.commit()

    # Step 2. Fetch CSV from GitHub
    print("📥 Fetching CSV from GitHub...")
    response = requests.get(CSV_URL)
    response.raise_for_status()
    csv_data = response.text
    buffer = io.StringIO(csv_data)

    # Step 3. Copy into books_stage
    print("🚀 Loading raw CSV into staging table...")
    cur.copy_expert("""
        COPY books_stage(isbn, title, author, description, publication_date, cover_image_url, genre, rating)
        FROM STDIN
        WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');
    """, buffer)
    conn.commit()

    # Step 4. Insert clean data into main books table
    print("🧹 Transforming and inserting into books...")
    cur.execute("""
        INSERT INTO books (isbn, title, author, description, publication_date, cover_image_url, genre, rating)
        SELECT
            NULLIF(isbn, '')::BIGINT,
            NULLIF(title, ''),
            NULLIF(author, ''),
            NULLIF(description, ''),
            NULLIF(publication_date, '')::DATE,
            NULLIF(cover_image_url, ''),
            NULLIF(genre, ''),
            NULLIF(rating, '')::NUMERIC
        FROM books_stage;
    """)
    conn.commit()

    # Step 5. Drop staging table
    print("🧽 Dropping temporary staging table...")
    cur.execute("DROP TABLE IF EXISTS books_stage;")
    conn.commit()

    cur.close()
    conn.close()
    print(f"✅ Import completed in {(time.time() - start)/60:.2f} minutes.")

if __name__ == "__main__":
    import_books()