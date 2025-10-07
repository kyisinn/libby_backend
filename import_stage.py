import psycopg2
import requests
import io

conn = psycopg2.connect("postgresql://postgres:pAflkfysMwUFGUPGzcbLBfUvoVJJjazQ@postgres.railway.internal:5432/railway")
cur = conn.cursor()

# Get the CSV directly from GitHub
url = "https://raw.githubusercontent.com/kyisinn/Data/main/AU_Library_Books_Finalv2.csv"
csv_data = requests.get(url).text
buffer = io.StringIO(csv_data)

# Copy from buffer into books table (skip book_id)
cur.copy_expert("""
    COPY books(isbn, title, author, description, publication_date, cover_image_url, genre, rating)
    FROM STDIN
    WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');
""", buffer)

conn.commit()
cur.close()
conn.close()
print("✅ Import completed successfully!")