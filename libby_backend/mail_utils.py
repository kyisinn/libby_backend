import os
import logging
import requests
from flask_mail import Message
from libby_backend.extensions import mail

logger = logging.getLogger(__name__)

def send_html_email(to_email: str, subject: str, html: str):
    """
    Send an HTML email using Flask-Mail (SMTP).
    Falls back to Resend API if SMTP fails (useful for Railway deployment).
    """
    # Try Gmail SMTP first
    try:
        msg = Message(
            subject=subject,
            recipients=[to_email],
            html=html,
            sender=os.getenv("SENDER_EMAIL")
        )
        mail.send(msg)
        print(f"✅ Gmail SMTP email sent to {to_email}")
        logger.info(f"Email successfully sent via SMTP to {to_email}")
        return {"success": True, "method": "smtp"}
    except Exception as smtp_err:
        print(f"⚠️ Gmail SMTP failed: {smtp_err}")
        logger.warning(f"SMTP failed for {to_email}: {smtp_err}")
        
        # --- Try Resend API fallback ---
        try:
            api_key = os.getenv("RESEND_API_KEY")
            sender_name = os.getenv("SENDER_NAME", "AU Bibliophiles")
            
            if not api_key:
                raise ValueError("RESEND_API_KEY not found in environment")
            
            #  Resend subdomain or custom verified domain
            resend_sender_email = os.getenv("RESEND_SENDER_EMAIL", "noreply@au-libbybot.on.resend.dev")
            resend_sender = f"{sender_name} <{resend_sender_email}>"
            
            response = requests.post(
                "https://api.resend.com/emails",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "from": resend_sender,
                    "to": [to_email],
                    "subject": subject,
                    "html": html  # Send HTML content
                }
            )
            
            if response.status_code == 200:
                print(f"✅ Sent via Resend API to {to_email}")
                logger.info(f"Email successfully sent via Resend API to {to_email}")
                return {"success": True, "method": "resend"}
            else:
                print(f"❌ Resend API failed: {response.text}")
                logger.error(f"Resend API failed for {to_email}: {response.text}")
                raise Exception(f"Resend API error: {response.text}")
                
        except Exception as resend_err:
            print(f"❌ Resend fallback error: {resend_err}")
            logger.error(f"Both SMTP and Resend failed for {to_email}: {resend_err}")
            raise Exception(f"All email methods failed. SMTP: {smtp_err}, Resend: {resend_err}")
