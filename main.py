import os
import requests as requests
from datetime import date
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import pandas as pd

from settings import sender_email, sender_password, receiver_email, smtp_server, report_app_file_url,\
    report_web_file_url, file_path, email_subject


today = date.today().strftime("%d.%m.%Y")

current_web_file_name = f'web_moderation_{today}.csv'
current_web_file_path = f'{file_path}/{today}/{current_web_file_name}'
current_app_file_name = f'app_moderation_{today}.csv'
current_app_file_path = f'{file_path}/{today}/{current_app_file_name}'
current_email_subject = f'{email_subject} {today}'


def main():
    os.mkdir(f'{file_path}\\{today}')
    app_response = requests.get(report_app_file_url)
    web_response = requests.get(report_web_file_url)
    print(current_web_file_name)
    print(current_app_file_name)

    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = receiver_email
    message['Subject'] = current_email_subject

    app_attachment = MIMEBase('application', 'octet-stream')
    web_attachment = MIMEBase('application', 'octet-stream')
    with open(current_web_file_path, 'wb') as file:
        file.write(web_response.content)

    with open(current_web_file_path, 'rb') as file:
        df = pd.read_csv(file)
        if len(df.index):  # Условие проверки на не-пустой файл
            web_attachment.set_payload(file.read())
            print(df.head())
            print(f'Файл {current_web_file_path} успешно создан')
            encoders.encode_base64(web_attachment)
            web_attachment.add_header('Content-Disposition', f'attachment; filename={current_web_file_name}')
            message.attach(web_attachment)
        else:
            print(f'C файлом {current_web_file_path} что-то не так, проверьте')
    with open(current_app_file_path, 'wb') as file:
        file.write(app_response.content)
    with open(current_app_file_path, 'rb') as file:
        df = pd.read_csv(file)
        if len(df.index):  # Условие проверки на не-пустой файл
            app_attachment.set_payload(file.read())
            print(df.head())
            print(f'Файл {current_app_file_path} успешно создан')
            encoders.encode_base64(app_attachment)
            app_attachment.add_header('Content-Disposition', f'attachment; filename={current_app_file_name}')
            message.attach(app_attachment)
        else:
            print(f'C файлом {current_app_file_path} что-то не так, проверьте')

    with smtplib.SMTP(smtp_server, 587) as server:
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, receiver_email, message.as_string())
        print(f'Email "{current_email_subject}" отправлен')

    print('Нажмите что-то для завершения работы скрипта')
    input()


if __name__ == '__main__':
    if os.path.exists(f'{file_path}/{today}'):
        print('Отчет уже сформирован и должен был быть отправлен. Если этого не произошло, повторите отправку вручную')
    else:
        main()
