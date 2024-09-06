import os
import requests as requests
from datetime import date
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import pandas as pd

import settings


today = date.today().strftime("%d.%m.%Y")

other_web_file_name = f'web_moderation_{today}.csv'
other_web_file_path = f'{settings.file_path}/{today}/{other_web_file_name}'
other_app_file_name = f'app_moderation_{today}.csv'
other_app_file_path = f'{settings.file_path}/{today}/{other_app_file_name}'
email_subject = settings.email_subject

file_path = f'{settings.file_path}//{today}'


def process_and_attach(message, path, response, name):
    with open(path, 'wb') as file:
        file.write(response.content)

    attachment = MIMEBase('application', 'octet-stream')

    with open(path, 'rb') as file:
        df = pd.read_csv(file)
        if len(df.index):  # Условие проверки на не-пустой файл
            file.seek(0)
            attachment.set_payload(file.read())
            print(df.head())
            print(f'Файл {path} успешно создан')
            encoders.encode_base64(attachment)
            attachment.add_header('Content-Disposition', f'attachment; filename={name}')
            message.attach(attachment)
        else:
            print(f'C файлом {path} что-то не так, проверьте')


def main():
    os.mkdir(f'{settings.file_path}\\{today}')
    other_web_response = requests.get(settings.report_web_file_other_url)
    other_app_response = requests.get(settings.report_app_file_other_url)

    message = MIMEMultipart()
    message['From'] = settings.sender_email
    message['To'] = settings.receiver_email
    message['Subject'] = email_subject

    process_and_attach(message, other_app_file_path, other_app_response, other_app_file_name)
    process_and_attach(message, other_web_file_path, other_web_response, other_web_file_name)

    with smtplib.SMTP(settings.smtp_server, 587) as server:
        server.starttls()
        server.login(settings.sender_email, settings.sender_password)
        server.sendmail(settings.sender_email, settings.receiver_email, message.as_string())
        print(f'Email "{email_subject}" отправлен')

    print('Нажмите что-то для завершения работы скрипта')
    input()


if __name__ == '__main__':
    if os.path.exists(f'{settings.file_path}/{today}'):
        print('Отчет уже сформирован и должен был быть отправлен. Если этого не произошло, повторите отправку вручную')
    else:
        main()
