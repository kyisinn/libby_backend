from flask import Blueprint, request, jsonify
from psycopg2.extras import RealDictCursor
from libby_backend.database import get_db_connection
from libby_backend.digests import send_digest_for_user  # returns bool or (bool, reason)
import logging

prefs_bp = Blueprint("prefs", __name__, url_prefix="/api/notify")
VALID = {"weekly", "monthly", "none"}


def _primary_email_from_clerk(clerk_user_id: str) -> str | None:
    """Attempt to read a primary email from the local `public.users` table.

    This is a cheap backfill used when users_with_preferences has no email yet.
    Returns an email string or None on any failure.
    """
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT email
                  FROM public.users
                 WHERE clerk_user_id = %s
                 LIMIT 1
            """, (clerk_user_id,))
            row = cur.fetchone()
            if not row:
                return None
            return row.get("email")
    except Exception:
        logging.getLogger(__name__).exception("_primary_email_from_clerk failed")
        return None


@prefs_bp.route("/email", methods=["POST"])
def upsert_pref_and_send_now():
    try:
        data = request.get_json(silent=True) or {}
        clerk_user_id = (data.get("clerk_user_id") or "").strip()
        freq = (data.get("frequency") or "").lower()
        if not clerk_user_id or freq not in {"weekly","monthly","none"}:
            return jsonify({"ok": False, "error": "bad_input"}), 400

        conn = get_db_connection()

        # 1) upsert pref and fetch current email (if present)
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
              INSERT INTO public.users_with_preferences (clerk_user_id, email_frequency)
              VALUES (%s, %s)
              ON CONFLICT (clerk_user_id) DO UPDATE SET email_frequency = EXCLUDED.email_frequency
              RETURNING email
            """, (clerk_user_id, freq))
            row = cur.fetchone() or {}
            email = row.get("email")

        # 2) if weekly/monthly must have email; try Clerk backfill once
        send_now = None
        if freq in ("weekly","monthly"):
            if not email:
                # attempt to backfill primary email from Clerk service/helper
                # helper `_primary_email_from_clerk` should return an email or None
                email = _primary_email_from_clerk(clerk_user_id)
                if email:
                    with conn, conn.cursor() as cur:
                        cur.execute("""
                          UPDATE public.users_with_preferences SET email=%s
                          WHERE clerk_user_id=%s
                        """, (email, clerk_user_id))
            if not email:
                return jsonify({"ok": True, "saved_frequency": freq,
                                "send_now": {"ok": False, "reason": "no_email"}}), 200

            ok, reason = send_digest_for_user(clerk_user_id)
            send_now = {"ok": bool(ok), "reason": reason}
            if ok:
                with conn, conn.cursor() as cur:
                    cur.execute("""
                      UPDATE public.users_with_preferences
                         SET last_digest_sent_at = NOW()
                       WHERE clerk_user_id = %s
                    """, (clerk_user_id,))

        return jsonify({"ok": True, "saved_frequency": freq, "send_now": send_now}), 200

    except Exception as e:
        # log full stack to server logs and return compact reason to client
        import traceback, logging
        logging.getLogger(__name__).exception("notify/email failed")
        return jsonify({"ok": False, "error": "internal_error", "detail": type(e).__name__}), 500


@prefs_bp.route("/email", methods=["GET"])
def get_email_pref():
    clerk_user_id = request.args.get("clerk_user_id", "").strip()
    if not clerk_user_id:
        return jsonify({"ok": False, "error": "Missing clerk_user_id"}), 400

    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT email_frequency, last_digest_sent_at, email
              FROM public.users_with_preferences
             WHERE clerk_user_id = %s
             LIMIT 1
        """, (clerk_user_id,))
        row = cur.fetchone()
        if not row:
            # create a default row so UI always works
            with conn, conn.cursor() as cur2:
                cur2.execute("""
                  INSERT INTO public.users_with_preferences (clerk_user_id, email_frequency)
                  VALUES (%s, 'none')
                  ON CONFLICT (clerk_user_id) DO NOTHING
                """, (clerk_user_id,))
            return jsonify({"ok": True, "email_frequency": "none", "last_digest_sent_at": None, "email": None})
        
        # Convert row to dict and check if email is missing
        result = dict(row)
        result["ok"] = True
        result["email_missing"] = not result.get("email")
        return jsonify(result)
    return jsonify({"ok": True, **row})


@prefs_bp.route("/email/update", methods=["POST"])
def update_user_email():
    """Update or add email for a user in users_with_preferences table."""
    try:
        data = request.get_json(silent=True) or {}
        clerk_user_id = (data.get("clerk_user_id") or "").strip()
        email = (data.get("email") or "").strip()
        
        if not clerk_user_id:
            return jsonify({"ok": False, "error": "Missing clerk_user_id"}), 400
        if not email:
            return jsonify({"ok": False, "error": "Missing email"}), 400
        
        # Basic email validation
        if "@" not in email or "." not in email.split("@")[1]:
            return jsonify({"ok": False, "error": "Invalid email format"}), 400
        
        conn = get_db_connection()
        
        # Update email in both users_with_preferences and users tables
        with conn, conn.cursor() as cur:
            # Update users_with_preferences
            cur.execute("""
                INSERT INTO public.users_with_preferences (clerk_user_id, email, email_frequency)
                VALUES (%s, %s, 'none')
                ON CONFLICT (clerk_user_id) DO UPDATE 
                SET email = EXCLUDED.email
            """, (clerk_user_id, email))
            
            # Also update users table if the user exists there
            cur.execute("""
                UPDATE public.users 
                SET email = %s, updated_at = CURRENT_TIMESTAMP
                WHERE clerk_user_id = %s
            """, (email, clerk_user_id))
        
        return jsonify({"ok": True, "email": email, "message": "Email updated successfully"}), 200
        
    except Exception as e:
        import traceback, logging
        logging.getLogger(__name__).exception("notify/email/update failed")
        return jsonify({"ok": False, "error": "internal_error", "detail": type(e).__name__}), 500
