import os
import logging
from flask_mail import Message
from libby_backend.extensions import mail

logger = logging.getLogger(__name__)

def send_html_email(to_email: str, subject: str, html: str):
    """Send an HTML email using Flask-Mail."""
    try:
        msg = Message(
            subject=subject,
            recipients=[to_email],
            html=html,
            sender=os.getenv("SENDER_EMAIL")
        )
        mail.send(msg)
        print(f"✅ Email successfully sent to {to_email}")
        logger.info(f"Email successfully sent to {to_email}")
        return {"success": True}
    except Exception as e:
        import traceback
        print("❌ Error sending email:")
        print(traceback.format_exc())
        logger.error(f"Failed to send email to {to_email}: {e}")
        raise
