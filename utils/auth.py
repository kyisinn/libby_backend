import os, datetime, jwt
from functools import wraps
from flask import request, jsonify, g, current_app

def create_jwt(user_id: int, email: str) -> str:
    days = int(current_app.config["JWT_EXPIRES_DAYS"])
    payload = {
        "sub": user_id,
        "email": email,
        "iat": datetime.datetime.utcnow(),
        "exp": datetime.datetime.utcnow() + datetime.timedelta(days=days),
    }
    return jwt.encode(payload, current_app.config["JWT_SECRET"], algorithm="HS256")

def set_jwt_cookie(response, user_id: int, email: str):
    token = create_jwt(user_id, email)
    is_prod = current_app.config["FLASK_ENV"] == "production"
    response.set_cookie(
        "jwt_token",
        token,
        httponly=True,
        secure=is_prod,
        samesite="None" if is_prod else "Lax",
        max_age=int(current_app.config["JWT_EXPIRES_DAYS"]) * 86400,
        path="/",
    )
    return response

def clear_jwt_cookie(response):
    is_prod = current_app.config["FLASK_ENV"] == "production"
    response.set_cookie(
        "jwt_token", "", expires=0, httponly=True, secure=is_prod,
        samesite="None" if is_prod else "Lax", path="/"
    )
    return response

def _resolve_bearer_from_headers() -> str | None:
    auth_header = (
        request.headers.get("Authorization")
        or request.headers.get("authorization")
        or request.headers.get("X-Authorization")
        or request.headers.get("X-Forwarded-Authorization")
        or request.headers.get("Http-Authorization")
        or request.headers.get("HTTP_AUTHORIZATION")
        or request.environ.get("HTTP_AUTHORIZATION")
        or ""
    ).strip()
    if not auth_header:
        return None
    parts = auth_header.split()
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].strip()
    return None

def auth_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        token = request.cookies.get("jwt_token") or _resolve_bearer_from_headers()
        if not token:
            return jsonify({"error": "Authentication required"}), 401
        try:
            payload = jwt.decode(token, current_app.config["JWT_SECRET"], algorithms=["HS256"])
            g.user_id = payload.get("sub")
            g.email = payload.get("email")
            if not g.user_id:
                return jsonify({"error": "Invalid token"}), 401
        except jwt.ExpiredSignatureError:
            return jsonify({"error": "Token expired"}), 401
        except jwt.InvalidSignatureError:
            return jsonify({"error": "Invalid token signature"}), 401
        except Exception:
            return jsonify({"error": "Invalid token"}), 401
        return fn(*args, **kwargs)
    return wrapper