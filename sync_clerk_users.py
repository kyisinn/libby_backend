# sync_clerk_users.py
import os, time, requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from dotenv import load_dotenv


load_dotenv()
load_dotenv("env")

CLERK_KEY = os.getenv("CLERK_SECRET_KEY")
DB_URL    = os.getenv("DATABASE_URL")
if not CLERK_KEY or not DB_URL:
    raise SystemExit("Missing CLERK_SECRET_KEY or DATABASE_URL")

# ---- Helpers ---------------------------------------------------------------
def to_ts(s):
    if not s:
        return None
    try:
        # Clerk returns ISO8601 strings, e.g., "2025-09-25T12:34:56.123Z"
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def primary_email(user):
    pid = user.get("primary_email_address_id")
    emails = user.get("email_addresses") or []
    for e in emails:
        if e.get("id") == pid:
            return e.get("email_address")
    return emails[0]["email_address"] if emails else None

# ---- Fetch all Clerk users (paged) ----------------------------------------
def fetch_clerk_users(limit=100):
    url = "https://api.clerk.com/v1/users"
    headers = {"Authorization": f"Bearer {CLERK_KEY}"}
    offset = 0
    while True:
        params = {"limit": limit, "offset": offset, "order_by": "-created_at"}
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        batch = r.json()
        if not isinstance(batch, list):
            # some SDKs return {data: [...]}; handle both
            batch = batch.get("data", [])
        if not batch:
            break
        yield from batch
        if len(batch) < limit:
            break
        offset += limit
        time.sleep(0.2)  # be nice to API

# ---- Upsert into Postgres --------------------------------------------------
UPSERT_SQL = """
INSERT INTO public.users_with_preferences
    (clerk_user_id, username, first_name, last_name, email, image_url, created_at, last_digest_sent_at, email_frequency)
VALUES %s
ON CONFLICT (clerk_user_id) DO UPDATE SET
    username = EXCLUDED.username,
    first_name = EXCLUDED.first_name,
    last_name  = EXCLUDED.last_name,
    email      = EXCLUDED.email,
    image_url  = EXCLUDED.image_url,
    created_at = COALESCE(public.users_with_preferences.created_at, EXCLUDED.created_at)
-- keep your existing email_frequency/last_digest_sent_at unless they are NULL
;
"""

def main():
    rows = []
    for u in fetch_clerk_users():
        rows.append((
            u["id"],                              # clerk_user_id
            (u.get("username") or "")[:255],      # username
            (u.get("first_name") or "")[:255],
            (u.get("last_name") or "")[:255],
            primary_email(u),
            (u.get("image_url") or None),
            to_ts(u.get("created_at")),
            None,            # last_digest_sent_at (leave None on first import)
            "none",          # email_frequency default; user can change later
        ))

    if not rows:
        print("No users fetched from Clerk.")
        return

    conn = psycopg2.connect(DB_URL)
    try:
        with conn, conn.cursor() as cur:
            execute_values(cur, UPSERT_SQL, rows, page_size=500)
        print(f"Upserted {len(rows)} users into users_with_preferences.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
