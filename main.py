from email.mime.application import MIMEApplication

import requests as requests
from datetime import date
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

from settings import sender_email, sender_password, receiver_email, smtp_server, report_file_url, file_path, \
    email_subject


today = date.today().strftime("%d.%m.%Y")

current_file_name = f'web_moderation_{today}.csv'
current_file_path = f'{file_path}_{current_file_name}'
current_email_subject = f'{email_subject} {today}'


def main():
    response = requests.get(report_file_url)
    print(current_file_name)

    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = receiver_email
    message['Subject'] = current_email_subject

    attachment = MIMEBase('application', 'octet-stream')
    with open(current_file_path, 'wb') as file:
        file.write(response.content)
    with open(current_file_path, 'rb') as file:
        attachment.set_payload(file.read())
    print(f'Файл {current_file_path} успешно создан')
    encoders.encode_base64(attachment)
    attachment.add_header('Content-Disposition', f'attachment; filename={current_file_name}')
    message.attach(attachment)

    with smtplib.SMTP(smtp_server, 587) as server:
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, receiver_email, message.as_string())
        print(f'Email "{current_email_subject}" отправлен')

    print('Нажмите что-то для завершения работы скрипта')
    death = input()

if __name__ == '__main__':
    main()
