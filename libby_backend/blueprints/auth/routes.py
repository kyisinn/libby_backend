from flask import Blueprint, jsonify, request, g
from werkzeug.security import generate_password_hash, check_password_hash
from libby_backend.database import get_user_by_email, create_user, get_user_by_id
from ...utils.auth import auth_required, set_jwt_cookie, clear_jwt_cookie

bp = Blueprint("auth", __name__, url_prefix="/api/auth")

@bp.post("/signup")
def signup():
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip()
    password = data.get("password") or ""
    first_name = (data.get("first_name") or "").strip() or None
    last_name  = (data.get("last_name") or "").strip() or None
    phone      = (data.get("phone") or "").strip() or None

    if not email or not password:
        return jsonify({"error": "Email and password are required"}), 400
    if len(password) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400

    if get_user_by_email(email):
        return jsonify({"error": "Email already registered"}), 400

    pwd_hash = generate_password_hash(password)
    created = create_user(email, pwd_hash, first_name, last_name, phone)
    if created is None or (isinstance(created, dict) and created.get("_duplicate")):
        return jsonify({"error": "Failed to create user"}), 500

    resp = jsonify({"message": "User created"})
    return set_jwt_cookie(resp, created["user_id"], created["email"])

@bp.post("/login")
def login():
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip()
    password = data.get("password") or ""
    if not email or not password:
        return jsonify({"error": "Email and password are required"}), 400

    user = get_user_by_email(email)
    if not user or not check_password_hash(user["password_hash"], password):
        return jsonify({"error": "Invalid email or password"}), 401

    resp = jsonify({"message": "Login successful"})
    return set_jwt_cookie(resp, user["user_id"], user["email"])

@bp.post("/logout")
@auth_required
def logout():
    resp = jsonify({"message": "Logged out"})
    return clear_jwt_cookie(resp)

@bp.get("/me")
@auth_required
def me():
    user = get_user_by_id(g.user_id)
    if not user:
        return jsonify({"error": "User not found"}), 404
    return jsonify({
        "user_id": user["user_id"],
        "email": user["email"],
        "first_name": user.get("first_name"),
        "last_name": user.get("last_name"),
        "phone": user.get("phone"),
        "membership_type": user.get("membership_type"),
        "is_active": user.get("is_active"),
        "created_at": user.get("created_at").isoformat() if user.get("created_at") else None
    })