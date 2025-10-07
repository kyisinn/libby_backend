"""
One-time import script with staging table.
PostgreSQL-safe version — handles quotes, 9 columns, and keeps book_id from CSV.
"""

# NOTE: Ensure the 'publication_date' column in 'books' table is INTEGER (year only).
# Run this in pgAdmin before importing:
# ALTER TABLE public.books ALTER COLUMN book_id DROP DEFAULT;
# ALTER TABLE public.books ALTER COLUMN book_id TYPE BIGINT USING (book_id::BIGINT);
# ALTER TABLE public.books ALTER COLUMN publication_date TYPE INTEGER USING (publication_date::INTEGER);

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

    # Step 1. Check if main table already has data
    cur.execute("SELECT COUNT(*) FROM books;")
    count = cur.fetchone()[0]
    if count > 0:
        print(f"⚠️ Books table already has {count} rows — skipping import.")
        conn.close()
        return

    # Step 2. Create temporary staging table (matches CSV: 9 columns)
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

    # Step 3. Fetch CSV from GitHub
    print("📥 Fetching CSV from GitHub...")
    response = requests.get(CSV_URL)
    response.raise_for_status()
    csv_data = response.text.replace('\r', '').replace('"""', '"')

    # Step 4. Parse safely using Python's CSV reader
    reader = csv.reader(io.StringIO(csv_data))
    rows = list(reader)
    print(f"📄 Loaded {len(rows)} total rows (including header).")

    # Step 5. Clean & escape data for PostgreSQL
    header = rows[0]
    expected_cols = len(header)
    cleaned_lines = [','.join(header)]

    for row in rows[1:]:
        if not any(row):
            continue  # skip empty lines

        # Pad or trim to expected columns
        if len(row) > expected_cols:
            row = row[:expected_cols]
        elif len(row) < expected_cols:
            row += [''] * (expected_cols - len(row))

        # --- Clean numeric artifacts ---
        # book_id: remove .0 if exists
        if row[0].strip().endswith('.0'):
            row[0] = row[0].strip().split('.')[0]

        # publication_date: remove .0 if exists
        if len(row) > 5 and row[5].strip().endswith('.0'):
            row[5] = row[5].strip().split('.')[0]

        # --- Escape inner quotes for PostgreSQL ---
        row = [v.replace('"', '""') for v in row]

        # --- Join safely ---
        cleaned_lines.append(','.join(f'"{v}"' for v in row))

    # Combine into buffer
    csv_data = '\n'.join(cleaned_lines)
    buffer = io.StringIO(csv_data)
    print(f"✅ Cleaned {len(cleaned_lines)-1} data rows ready for import.")

    # Step 6. Copy into books_stage
    print("🚀 Loading data into staging table...")
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

    # Step 7. Normalize book_id before inserting into main table
    print("🔢 Normalizing book_id values...")
    cur.execute("UPDATE books_stage SET book_id = SPLIT_PART(book_id, '.', 1);")
    conn.commit()

    # Step 8. Insert into main books table (keep book_id from CSV)
    print("🧹 Transforming and inserting into main books table...")
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

    # Step 9. Drop temporary table
    print("🧽 Dropping temporary staging table...")
    cur.execute("DROP TABLE IF EXISTS books_stage;")
    conn.commit()

    cur.close()
    conn.close()
    runtime = (time.time() - start) / 60
    print(f"✅ Import completed successfully in {runtime:.2f} minutes.")

if __name__ == "__main__":
    import_books()