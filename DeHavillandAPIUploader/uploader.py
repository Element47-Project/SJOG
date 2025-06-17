import os
import logging
import smtplib
from email.message import EmailMessage
from dotenv import load_dotenv
from API import PowerLedgerUploader
import sys


def resource_path(relative_path):
    """兼容 PyInstaller 打包后的路径"""
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath("."), relative_path)


load_dotenv(resource_path("config.env"))

EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("PASSWORD")
RECEIVE_EMAIL = os.getenv("RECEIVE_EMAIL")
RECIPIENTS = [EMAIL_ADDRESS, RECEIVE_EMAIL]


def upload_and_notify(file_path: str, start_time: str, end_time: str):
    """
    Upload the given CSV file and send a notification email to stakeholders.
    """
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return

    try:
        print("🔁 Authenticating and uploading to PowerLedger...")
        uploader = PowerLedgerUploader(file_path=file_path)
        uploader.authenticate()
        uploader.process_and_upload()
        print(f"✅ File uploaded successfully: {os.path.basename(file_path)}")
        logging.info(f"Uploaded file: {file_path}")

        # Send email notification
        send_upload_notification_email(start_time, end_time)

    except Exception as e:
        logging.error(f"❌ Upload failed: {e}")
        print(f"❌ Upload failed: {e}")


def send_upload_notification_email(start_time: str, end_time: str):
    """
    Send a summary email after upload.
    """
    msg = EmailMessage()
    msg['Subject'] = f"[DeHavilland] Data Upload Completed ({start_time} to {end_time})"
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = ", ".join(RECIPIENTS)

    msg.set_content(f"""
Date Range:
Start: {start_time}
End:   {end_time}


Best regards,  
DeHavilland Automation System
""")

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            smtp.send_message(msg)
            print("✅ Upload notification email sent.")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
