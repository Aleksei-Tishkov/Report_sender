import os
import dotenv


dotenv.load_dotenv()

receiver_email = os.getenv('receiver_email')
sender_email = os.getenv('sender_email')
sender_password = os.getenv('GMAIL_APP_CODE')

smtp_server = 'smtp.gmail.com'
email_subject = os.getenv('email_subject')

report_app_file_url = os.getenv('REPORT_FILE_INAPP')
report_web_file_url = os.getenv('REPORT_FILE_WEB')

file_path = os.getenv('file_path')