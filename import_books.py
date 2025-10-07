"""
One-time import script with staging table.
This version is production-safe for Railway deployment.
"""

# NOTE: Ensure the 'publication_date' column in the 'books' table is of type INTEGER (year only).
# Run:
# ALTER TABLE public.books DROP COLUMN IF EXISTS publication_date;
# ALTER TABLE public.books ADD COLUMN publication_date INTEGER;

import psycopg2
import requests
import io
import time
import csv

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
            book_id TEXT,
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

    # 🧹 Clean malformed CSV text
    csv_data = response.text.replace('\r', '').replace('"""', '"')
    reader = csv.reader(io.StringIO(csv_data))
    rows = list(reader)

    # Determine header and expected columns
    header = rows[0]
    expected_cols = len(header)
    cleaned_lines = [','.join(header)]

    for row in rows[1:]:
        if not any(row):
            continue  # skip empty lines

        # If row too short or too long, try to fix
        if len(row) != expected_cols:
            print(f"⚠️ Attempting to fix malformed row ({len(row)} cols): {row[:3]}...")
            # Join back extra columns into the description field (usually at index 3 or 4)
            if len(row) > expected_cols:
                # merge excess text fields back into description
                fixed = row[:expected_cols-1]
                fixed[-1] = ','.join(row[expected_cols-1:])  # merge all extras
                row = fixed
            elif len(row) < expected_cols:
                # pad missing columns
                row += [''] * (expected_cols - len(row))
            else:
                continue

        # Fix year floats like 2010.0 → 2010
        if len(row) > 5 and row[5].strip().endswith('.0'):
            yr = row[5].strip().split('.')[0]
            row[5] = yr

        cleaned_lines.append(','.join(f'"{v.replace("\"", "\"\"")}"' for v in row))

    csv_data = '\n'.join(cleaned_lines)
    buffer = io.StringIO(csv_data)
    print(f"✅ Cleaned {len(cleaned_lines)-1} rows ready for import.")

    # Step 3. Copy into books_stage
    print("🚀 Loading raw CSV into staging table...")
    cur.copy_expert("""
        COPY books_stage(book_id, isbn, title, author, description, publication_date, cover_image_url, genre, rating)
        FROM STDIN
        WITH (
            FORMAT CSV,
            HEADER TRUE,
            DELIMITER ',',
            QUOTE '"',
            ESCAPE '"',
            ENCODING 'UTF8'
        );
    """, buffer)
    conn.commit()

    # Step 4. Insert clean data into main books table
    print("🧹 Transforming and inserting into books...")
    cur.execute("""
        INSERT INTO books (book_id, isbn, title, author, description, publication_date, cover_image_url, genre, rating)
        SELECT
            NULLIF(book_id, '')::BIGINT,
            NULLIF(isbn, '')::BIGINT,
            NULLIF(title, ''),
            NULLIF(author, ''),
            NULLIF(description, ''),
            NULLIF(publication_date, '')::INT,
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