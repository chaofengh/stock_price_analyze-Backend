# routes/user_routes.py
import os
import re
import smtplib
from datetime import datetime
from flask import Blueprint, request, jsonify
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from database.user_repository import (
    create_user,
    find_user_by_email_or_username,
    find_user_by_email,
    verify_password,
    set_reset_token
)

user_blueprint = Blueprint("user_routes", __name__)

# For demonstration, we'll read email server configs from environment variables:
MAIL_SERVER = os.getenv("MAIL_SERVER", "smtp.gmail.com")
MAIL_PORT = int(os.getenv("MAIL_PORT", 587))
MAIL_USERNAME = os.getenv("MAIL_USERNAME")
MAIL_PASSWORD = os.getenv("MAIL_PASSWORD")
MAIL_USE_TLS = os.getenv("MAIL_USE_TLS", "False").lower() == "true"

def sanitize_username(username: str) -> str:
    """
    Example 'sanitization' by removing anything not alphanumeric or underscore.
    """
    return re.sub(r'[^a-zA-Z0-9_]', '', username)

@user_blueprint.route("/register", methods=["POST"])
def register():
    """
    Registers a new user.
    Expects JSON: { "email": "...", "username": "...", "password": "..." }
    """
    data = request.get_json() or {}
    email = (data.get("email") or "").strip()
    username = sanitize_username((data.get("username") or "").strip())
    password = (data.get("password") or "").strip()

    if not email or not username or not password:
        return jsonify({"error": "email, username, and password are required"}), 400
    
    # Basic server-side checks
    if len(username) < 3:
        return jsonify({"error": "Username must be at least 3 characters"}), 400
    if len(password) < 8:
        return jsonify({"error": "Password must be at least 8 characters"}), 400
    if "@" not in email:
        return jsonify({"error": "Invalid email format"}), 400
    
    # Attempt to create user
    try:
        user = create_user(email, username, password)
        return jsonify({
            "message": "User registered successfully",
            "user": {
                "id": user[0],
                "email": user[1],
                "username": user[2],
                "created_at": user[3].isoformat() if user[3] else None
            }
        }), 201
    except Exception as e:
        # If it's a unique constraint violation, handle gracefully
        err_msg = str(e)
        if "duplicate key" in err_msg.lower():
            return jsonify({"error": "User with that email or username already exists"}), 400
        return jsonify({"error": err_msg}), 500

@user_blueprint.route("/login", methods=["POST"])
def login():
    """
    Logs in an existing user.
    Expects JSON: { "email_or_username": "...", "password": "..." }
    """
    data = request.get_json() or {}
    email_or_username = (data.get("email_or_username") or "").strip()
    password = (data.get("password") or "").strip()

    if not email_or_username or not password:
        return jsonify({"error": "Missing email_or_username or password"}), 400

    user_row = find_user_by_email_or_username(email_or_username)
    if not user_row:
        return jsonify({"error": "Invalid credentials"}), 401
    
    # user_row format: (id, email, username, password_hash, reset_token, reset_token_expires)
    user_id, user_email, user_name, password_hash, reset_token, reset_expires = user_row

    if not verify_password(password, password_hash):
        return jsonify({"error": "Invalid credentials"}), 401

    # At this point, login is successful. You might create a session or JWT.
    # For simplicity, just return a success message.
    return jsonify({
        "message": "Login successful",
        "user": {
            "id": user_id,
            "email": user_email,
            "username": user_name
        }
    }), 200

@user_blueprint.route("/forgot_password", methods=["POST"])
def forgot_password():
    """
    Generates a reset token and sends an email to the user if they exist.
    Expects JSON: { "email": "..." }
    """
    data = request.get_json() or {}
    email = (data.get("email") or "").strip()
    if not email:
        return jsonify({"error": "Email is required"}), 400
    
    user_row = find_user_by_email(email)
    if not user_row:
        # For security, return a generic message
        return jsonify({"message": "If that email exists, a reset link was sent"}), 200
    
    user_id, user_email, user_name, password_hash, reset_token, reset_expires = user_row

    # Generate and store a new token
    new_token = set_reset_token(user_id)

    # Send the email
    try:
        send_reset_email(to_email=user_email, reset_token=new_token)
    except Exception as e:
        return jsonify({"error": f"Failed to send email: {str(e)}"}), 500

    return jsonify({"message": "If that email exists, a reset link was sent"}), 200

def send_reset_email(to_email: str, reset_token: str):
    """
    Send an email containing a reset link (or token).
    Here we demonstrate a simple SMTP approach; you could also use
    services like SendGrid, Mailgun, AWS SES, etc.
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
