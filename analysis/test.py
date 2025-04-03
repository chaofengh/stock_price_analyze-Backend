# tasks/textbelt_utils.py

import os
import requests
import logging
from dotenv import load_dotenv
load_dotenv()
logger = logging.getLogger(__name__)

def send_textbelt_sms(phone_number, message):
    """
    Sends a single SMS via TextBelt.
    """
    api_key = os.getenv("TEXTBELT_API_KEY", "")
    if not api_key:
        logger.error("No TEXTBELT_API_KEY found in environment variables.")
        return

    url = "https://textbelt.com/text"
    payload = {
        "phone": phone_number,
        "message": message,
        "key": api_key
    }
    try:
        resp = requests.post(url, data=payload)
        logger.info(f"TextBelt response: {resp.json()}")
    except Exception as e:
        logger.error(f"Error sending SMS: {e}")

send_textbelt_sms(3124518626,'test')