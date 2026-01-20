#user_routes.py
import os
import re
import smtplib
import jwt
from datetime import datetime, timezone, timedelta
from flask import Blueprint, request, jsonify
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from database.user_repository import (
    create_user,
    find_user_by_email_or_username,
    find_user_by_email,
    get_user_public_profile,
    verify_password,
    set_reset_token
)
from database.ticker_repository import create_default_user_list, create_empty_default_user_list


user_blueprint = Blueprint("user_routes", __name__)

# Email configuration from environment variables
MAIL_SERVER = os.getenv("MAIL_SERVER", "smtp.gmail.com")
MAIL_PORT = int(os.getenv("MAIL_PORT", 587))
MAIL_USERNAME = os.getenv("MAIL_USERNAME")
MAIL_PASSWORD = os.getenv("MAIL_PASSWORD")
MAIL_USE_TLS = os.getenv("MAIL_USE_TLS", "False").lower() == "true"

# JWT configuration
JWT_SECRET = os.getenv("JWT_SECRET", "your_jwt_secret")
JWT_ALGORITHM = "HS256"
JWT_ACCESS_TOKEN_EXPIRE_DAYS = int(os.getenv("JWT_ACCESS_TOKEN_EXPIRE_DAYS", "30"))

def sanitize_username(username: str) -> str:
    """
    Removes any character that's not alphanumeric or underscore.
    """
    return re.sub(r'[^a-zA-Z0-9_]', '', username)

def _parse_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "y", "on")
    return False

@user_blueprint.route("/register", methods=["POST"])
def register():
    """
    Registers a new user.
    Expects JSON:
    {
      "email": "...",
      "username": "...",
      "password": "...",
      "first_name": "...",          # optional
      "last_name": "...",           # optional
      "phone": "...",               # optional
      "country": "...",             # optional
      "timezone": "...",            # optional
      "marketing_opt_in": false,    # optional
      "honey_trap": "",
      "form_time": <time_in_seconds>
    }
    """
    data = request.get_json() or {}
    email = (data.get("email") or "").strip()
    username = sanitize_username((data.get("username") or "").strip())
    password = (data.get("password") or "").strip()
    first_name = (data.get("first_name") or "").strip() or None
    last_name = (data.get("last_name") or "").strip() or None
    phone = (data.get("phone") or "").strip() or None
    country = (data.get("country") or "").strip() or None
    user_timezone = (data.get("timezone") or "").strip() or None
    marketing_opt_in = _parse_bool(data.get("marketing_opt_in"))
    honey_trap = (data.get("honey_trap") or "").strip()
    form_time = data.get("form_time")

    if not email or not username or not password or honey_trap is None or form_time is None:
        return jsonify({"error": "email, username, password, honey_trap, and form_time are required"}), 400

    if honey_trap != "":
        return jsonify({"error": "Bot detected"}), 400

    try:
        form_time = float(form_time)
    except Exception:
        return jsonify({"error": "Invalid form time"}), 400

    if form_time < 5:
        return jsonify({"error": "Form submitted too quickly"}), 400

    if len(username) < 3:
        return jsonify({"error": "Username must be at least 3 characters"}), 400
    if len(password) < 8:
        return jsonify({"error": "Password must be at least 8 characters"}), 400
    if "@" not in email:
        return jsonify({"error": "Invalid email format"}), 400
    if first_name and len(first_name) > 80:
        return jsonify({"error": "First name is too long"}), 400
    if last_name and len(last_name) > 80:
        return jsonify({"error": "Last name is too long"}), 400
    if phone and len(phone) > 30:
        return jsonify({"error": "Phone is too long"}), 400
    if country and len(country) > 64:
        return jsonify({"error": "Country is too long"}), 400
    if user_timezone and len(user_timezone) > 64:
        return jsonify({"error": "Timezone is too long"}), 400

    try:
        # 1) Create the new user
        user = create_user(
            email=email,
            username=username,
            password=password,
            first_name=first_name,
            last_name=last_name,
            phone=phone,
            country=country,
            timezone=user_timezone,
            marketing_opt_in=marketing_opt_in,
        )
        (
            user_id,
            user_email,
            user_name,
            created_at,
            created_first_name,
            created_last_name,
            created_phone,
            created_country,
            created_timezone,
            created_marketing_opt_in,
        ) = user

        # 2) Create the default list for this user
        if user_email.lower() == "test@gmail.com":
            create_default_user_list(user_id)
        else:
            create_empty_default_user_list(user_id)

        payload = {
            "user_id": user_id,
            "iat": datetime.now(timezone.utc),
            "exp": datetime.now(timezone.utc) + timedelta(days=JWT_ACCESS_TOKEN_EXPIRE_DAYS),
        }
        token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

        return jsonify({
            "message": "User registered successfully",
            "user": {
                "id": user_id,
                "email": user_email,
                "username": user_name,
                "first_name": created_first_name,
                "last_name": created_last_name,
                "phone": created_phone,
                "country": created_country,
                "timezone": created_timezone,
                "marketing_opt_in": created_marketing_opt_in,
                "created_at": created_at.isoformat() if created_at else None
            },
            "token": token,
        }), 201
    except Exception as e:
        err_msg = str(e)
        if "duplicate key" in err_msg.lower():
            return jsonify({"error": "User with that email or username already exists"}), 400
        return jsonify({"error": err_msg}), 500


@user_blueprint.route("/login", methods=["POST"])
def login():
    """
    Logs in an existing user.
    Expects JSON:
    {
      "email_or_username": "...",
      "password": "...",
      "honey_trap": "",            # optional
      "form_time": <seconds>       # optional
    }
    On successful login, a JWT token is returned.
    """
    data = request.get_json() or {}
    email_or_username = (data.get("email_or_username") or "").strip()
    password = (data.get("password") or "").strip()
    honey_trap = (data.get("honey_trap") or "").strip()
    form_time = data.get("form_time")

    if not email_or_username or not password:
        return jsonify({"error": "email_or_username and password are required"}), 400

    if honey_trap != "":
        return jsonify({"error": "Bot detected"}), 400

    # form_time is optional for login; ignore invalid values for compatibility.
    if form_time is not None:
        try:
            float(form_time)
        except Exception:
            pass

    user_row = find_user_by_email_or_username(email_or_username)
    if not user_row:
        return jsonify({"error": "Invalid credentials"}), 401

    user_id, user_email, user_name, password_hash, _, _ = user_row

    if not verify_password(password, password_hash):
        return jsonify({"error": "Invalid credentials"}), 401

    if user_email.lower() == "test@gmail.com":
        try:
            create_default_user_list(user_id)
        except Exception:
            pass

    payload = {
        "user_id": user_id,
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(days=JWT_ACCESS_TOKEN_EXPIRE_DAYS),
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

    profile = get_user_public_profile(user_id)
    if profile:
        (
            _id,
            _email,
            _username,
            _created_at,
            _first_name,
            _last_name,
            _phone,
            _country,
            _timezone,
            _marketing_opt_in,
        ) = profile
        user_payload = {
            "id": _id,
            "email": _email,
            "username": _username,
            "first_name": _first_name,
            "last_name": _last_name,
            "phone": _phone,
            "country": _country,
            "timezone": _timezone,
            "marketing_opt_in": _marketing_opt_in,
        }
    else:
        user_payload = {
            "id": user_id,
            "email": user_email,
            "username": user_name,
        }

    return jsonify({
        "message": "Login successful",
        "user": user_payload,
        "token": token
    }), 200

@user_blueprint.route("/forgot_password", methods=["POST"])
def forgot_password():
    """
    Generates a reset token and sends an email to the user if they exist.
    Expects JSON:
    {
      "email": "...",
      "honey_trap": "",
      "form_time": <time_in_seconds>
    }
    """
    data = request.get_json() or {}
    email = (data.get("email") or "").strip()
    honey_trap = (data.get("honey_trap") or "").strip()
    form_time = data.get("form_time")

    if not email or honey_trap is None or form_time is None:
        return jsonify({"error": "Email, honey_trap, and form_time are required"}), 400

    if honey_trap != "":
        return jsonify({"error": "Bot detected"}), 400

    try:
        form_time = float(form_time)
    except Exception:
        return jsonify({"error": "Invalid form time"}), 400

    if form_time < 5:
        return jsonify({"error": "Form submitted too quickly"}), 400

    user_row = find_user_by_email(email)
    if not user_row:
        return jsonify({"message": "If that email exists, a reset link was sent"}), 200

    user_id, user_email, user_name, _, _, _ = user_row
    new_token = set_reset_token(user_id)

    try:
        send_reset_email(to_email=user_email, reset_token=new_token)
    except Exception as e:
        return jsonify({"error": f"Failed to send email: {str(e)}"}), 500

    return jsonify({"message": "If that email exists, a reset link was sent"}), 200

def send_reset_email(to_email: str, reset_token: str):
    """
    Sends an email containing the password reset link.
    This example uses Python's smtplib; in production, consider a dedicated email service.
    """
    subject = "Password Reset Request"
    reset_link = f"https://your-frontend-domain.com/reset_password?token={reset_token}"
    body = f"""
    Hello,

    We received a request to reset your password.
    Please use the following link (valid for 1 hour):
    {reset_link}

    If you did not request a password reset, you can ignore this email.
    """

    msg = MIMEMultipart()
    msg["From"] = MAIL_USERNAME
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    with smtplib.SMTP(MAIL_SERVER, MAIL_PORT) as server:
        if MAIL_USE_TLS:
            server.starttls()
        server.login(MAIL_USERNAME, MAIL_PASSWORD)
        server.send_message(msg)
