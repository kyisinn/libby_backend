from flask import Blueprint, request, jsonify
from psycopg2.extras import RealDictCursor
from libby_backend.database import get_db_connection
from libby_backend.digests import send_digest_for_user  # returns bool or (bool, reason)

prefs_bp = Blueprint("prefs", __name__, url_prefix="/api/notify")
VALID = {"weekly", "monthly", "none"}


@prefs_bp.route("/email", methods=["POST"])
def upsert_pref_and_send_now():
        data = request.get_json(silent=True) or {}
        clerk_user_id = (data.get("clerk_user_id") or "").strip()
        freq = (data.get("frequency") or "").lower()
        if not clerk_user_id or freq not in VALID:
                return jsonify({"ok": False, "error": "Provide clerk_user_id and frequency in ['weekly','monthly','none']"}), 400

        conn = get_db_connection()
        with conn, conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO public.users_with_preferences (clerk_user_id, email_frequency)
                    VALUES (%s, %s)
                    ON CONFLICT (clerk_user_id) DO UPDATE SET email_frequency = EXCLUDED.email_frequency
                """, (clerk_user_id, freq))

        send_now = None
        if freq in ("weekly", "monthly"):
                r = send_digest_for_user(clerk_user_id)
                ok, reason = (r if isinstance(r, tuple) else (bool(r), "sent" if r else "unknown"))
                send_now = {"ok": ok, "reason": reason}
                if ok:
                        with conn, conn.cursor() as cur:
                                cur.execute("""
                                    UPDATE public.users_with_preferences
                                         SET last_digest_sent_at = NOW()
                                     WHERE clerk_user_id = %s
                                """, (clerk_user_id,))

        return jsonify({"ok": True, "saved_frequency": freq, "send_now": send_now}), 200


@prefs_bp.route("/email", methods=["GET"])
def get_email_pref():
    clerk_user_id = request.args.get("clerk_user_id", "").strip()
    if not clerk_user_id:
        return jsonify({"ok": False, "error": "Missing clerk_user_id"}), 400

    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT email_frequency, last_digest_sent_at
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
            return jsonify({"ok": True, "email_frequency": "none", "last_digest_sent_at": None})
    return jsonify({"ok": True, **row})
