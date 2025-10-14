import os
import logging
from datetime import datetime, timezone
from psycopg2.extras import RealDictCursor
from typing import Any

from libby_backend.database import get_db_connection
from libby_backend.recommendation_system import EnhancedBookRecommendationEngine, UserProfile
from libby_backend.email_templates import au_bibliophiles_recs_html
from libby_backend.mail_utils import send_html_email

FRONTEND_BASE = os.getenv("FRONTEND_BASE", "https://libby-bot.vercel.app")
logger = logging.getLogger(__name__)


def _fetch_selected_genres(conn, clerk_user_id: str) -> list[str]:
    """Try to load user's selected genres; fall back to empty list if the table
    doesn't exist or the schema differs.

    Adjust the query if your project stores genres in a different table.
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT genre
                FROM public.user_interests
                WHERE clerk_user_id = %s
            """, (clerk_user_id,))
            rows = cur.fetchall() or []
            return [r["genre"] for r in rows if r.get("genre")]
    except Exception:
        return []


def _build_profile(clerk_user_id: str, email: str, selected_genres: list[str], user_id=None):
    """
    Build a UserProfile and only pass fields that the dataclass declares. The
    library's UserProfile requires at least 'email' and 'selected_genres' - we
    include them and conditionally include optional extras.
    """
    base: dict[str, Any] = {
        "clerk_user_id": clerk_user_id,
        "user_id": user_id,
        "email": email,
        "selected_genres": selected_genres,
        "preferred_rating_threshold": 3.8,
        "reading_history": [],
        "wishlist": [],
        "interests": [],
    }
    allowed = set(getattr(UserProfile, "__annotations__", {}).keys() or [])
    # If the dataclass has no annotations (unlikely), fall back to passing all.
    kwargs = {k: v for k, v in base.items() if (not allowed or k in allowed)}
    return UserProfile(**kwargs)


def _book_to_card(b: Any) -> dict:
    return {
        "id": getattr(b, "id", None),
        "title": getattr(b, "title", None) or getattr(b, "book_title", "Untitled"),
        "author": getattr(b, "author", None) or getattr(b, "author_name", "Unknown Author"),
        "cover_image_url": getattr(b, "cover_image_url", None),
        "blurb": getattr(b, "short_description", None) or getattr(b, "description", None) or "Your AU Bibliophiles Book Suggestions",
        "detail_url": f"{FRONTEND_BASE}/book/{getattr(b,'id','')}",
    }


def _is_due(freq: str, last, now: datetime) -> bool:
    if freq not in ("weekly", "monthly"):
        return False
    if last is None:
        return True
    days = (now - last).days
    return days >= (7 if freq == "weekly" else 28)


def send_digest_for_user(clerk_user_id: str):
    conn = get_db_connection()

    # 1) Load the recipient from users_with_preferences
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT email, first_name
            FROM public.users_with_preferences
            WHERE clerk_user_id = %s
            LIMIT 1
        """, (clerk_user_id,))
        user = cur.fetchone()

    if not user:
        return False, "no_user_row"
    if not user.get("email"):
        return False, "no_email"

    # 2) Load genres (or empty list)
    genres = _fetch_selected_genres(conn, clerk_user_id)

    # 3) Build the profile with required fields
    profile = _build_profile(
        clerk_user_id=clerk_user_id,
        email=user["email"],
        selected_genres=genres,
        user_id=None,
    )

    # 4) Generate recommendations and send
    engine = EnhancedBookRecommendationEngine()
    result = engine.hybrid_recommendations_enhanced(profile, total_limit=30)
    books = (getattr(result, "books", None) or [])[:4]
    if not books:
        return False, "no_books"

    html = au_bibliophiles_recs_html([
        _book_to_card(b) for b in books
    ], explore_url=f"{FRONTEND_BASE}app/recommendation")
    send_html_email(user["email"], "Recommended Reads Book Suggestions", html)

    # 5) Stamp last sent so cadence continues
    with conn, conn.cursor() as cur:
        cur.execute("""
            UPDATE public.users_with_preferences
               SET last_digest_sent_at = NOW()
             WHERE clerk_user_id = %s
        """, (clerk_user_id,))

    logger.info("Digest sent to %s", user["email"])
    return True, "sent"


def send_due_digests_batch() -> int:
    now = datetime.now(timezone.utc)
    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
          SELECT clerk_user_id, email_frequency, last_digest_sent_at
            FROM public.users_with_preferences
           WHERE email_frequency IN ('weekly','monthly')
        """)
        rows = cur.fetchall() or []

    sent = 0
    for r in rows:
        if _is_due(r["email_frequency"], r["last_digest_sent_at"], now):
            try:
                result = send_digest_for_user(r["clerk_user_id"])
                # sender returns (bool, reason) — accept either tuple or truthy bool
                ok = result if isinstance(result, bool) else (result[0] if isinstance(result, tuple) else bool(result))
                if ok:
                    sent += 1
            except Exception:
                logger.exception("Failed to send digest for %s", r["clerk_user_id"])

    return sent
