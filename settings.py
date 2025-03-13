import os
import dotenv

script_dir = os.path.dirname(os.path.realpath(__file__))
dotenv_path = os.path.join(script_dir, '.env')     # Prod
# dotenv_path = os.path.join(script_dir, '_.env')     # Test
dotenv.load_dotenv(dotenv_path)

receiver_email = os.getenv('RECEIVER_EMAIL')
sender_email = os.getenv('SENDER_EMAIL')
sender_password = os.getenv('GMAIL_APP_CODE')

smtp_server = 'smtp.gmail.com'
email_subject = os.getenv('EMAIL_SUBJECT')

report_web_file_weekends_url = os.getenv('REPORT_FILE_WEB_WEEKENDS')
report_app_file_weekends_url = os.getenv('REPORT_FILE_INAPP_WEEKENDS')

report_web_file_weekdays_url = os.getenv('REPORT_FILE_WEB_WEEKDAYS')
report_app_file_weekdays_url = os.getenv('REPORT_FILE_INAPP_WEEKDAYS')

crid_hashing_seed = int(os.getenv('CRID_HASHING_SEED'))

report_unmoderated = os.getenv('REPORT_UNMODERATED')

file_path = os.getenv('FILE_PATH')

message_high_text = os.getenv('MESSAGE_HIGH_TEXT')
message_low_text = os.getenv('MESSAGE_LOW_TEXT')

bot_token = os.getenv('BOT_TOKEN')
tg_recipient_id = os.getenv('TELEGRAM_RECIPIENT_ID')
