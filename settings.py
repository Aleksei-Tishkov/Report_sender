import os
import dotenv


dotenv.load_dotenv()

receiver_email = os.getenv('RECEIVER_EMAIL')
sender_email = os.getenv('SENDER_EMAIL')
sender_password = os.getenv('GMAIL_APP_CODE')

smtp_server = 'smtp.gmail.com'
email_subject = os.getenv('EMAIL_SUBJECT')

report_app_file_solta_weekdays_url = os.getenv('REPORT_FILE_INAPP_SOLTA_WEEKDAYS')
report_web_file_solta_weekdays_url = os.getenv('REPORT_FILE_WEB_SOLTA_WEEKDAYS')
report_app_file_solta_weekends_url = os.getenv('REPORT_FILE_INAPP_SOLTA_WEEKENDS')
report_web_file_solta_weekends_url = os.getenv('REPORT_FILE_WEB_SOLTA_WEEKENDS')

report_app_file_other_weekdays_url = os.getenv('REPORT_FILE_INAPP_OTHER_WEEKDAYS')
report_web_file_other_weekdays_url = os.getenv('REPORT_FILE_WEB_OTHER_WEEKDAYS')
report_app_file_other_weekends_url = os.getenv('REPORT_FILE_INAPP_OTHER_WEEKENDS')
report_web_file_other_weekends_url = os.getenv('REPORT_FILE_WEB_OTHER_WEEKENDS')

file_path = os.getenv('FILE_PATH')
