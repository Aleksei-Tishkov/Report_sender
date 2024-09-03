import os
import dotenv


dotenv.load_dotenv()

receiver_email = os.getenv('receiver_email')
sender_email = os.getenv('sender_email')
sender_password = os.getenv('GMAIL_APP_CODE')

smtp_server = 'smtp.gmail.com'
email_subject = os.getenv('email_subject')

report_file_url = os.getenv('REPORT_FILE')

file_path = os.getenv('file_path')