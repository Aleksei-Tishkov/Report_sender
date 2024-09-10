import os
import requests as requests
from datetime import date
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import pandas as pd
from io import StringIO

import settings


today = date.today()

if today.weekday() == 0:
    report_web_file_url = settings.report_web_file_weekends_url
    report_app_file_url = settings.report_app_file_weekends_url
else:
    report_web_file_url = settings.report_web_file_weekdays_url
    report_app_file_url = settings.report_app_file_weekdays_url

today = today.strftime("%d.%m.%Y")

email_subject = f'{settings.email_subject}{today}'

file_path = f'{settings.file_path}//{today}'

message_high_text = settings.message_high_text
message_low_text = settings.message_low_text


def check_and_attach(message, d):
    for filename, dataframe in d.items():
        if sum(dataframe.index) > 1:
            attachment = MIMEBase('application', 'octet-stream')
            dataframe.to_csv(f'{file_path}//{filename}_{today}.csv', index=False)
            csv_data = dataframe.to_csv(index=False)
            attachment.set_payload(csv_data.encode('utf-8'))
            encoders.encode_base64(attachment)
            attachment.add_header('Content-Disposition', f'attachment; filename="{filename}_{today}.csv"')
            message.attach(attachment)
        else:
            print(f'C файлом {filename} что-то не так, проверьте')


def process_and_attach(message_high, message_low, path, tp):
    for _ in range(3):
        response = requests.get(path)
        if response.status_code == 200:
            break
    else:
        raise BaseException('Ошибка доступа к серверу с отчетами')

    df = pd.read_csv(StringIO(response.text))

    df_solta = df[df['dsp'] == 'solta'].iloc[:, 1:]
    df_other = df[df['dsp'] == 'other'].iloc[:, 1:]

    df_other_high = df_other[df_other['count'] >= 100]
    df_other_low = df_other[df_other['count'] < 100]

    df_solta_high = df_solta[df_solta['count'] >= 25]
    df_solta_low = df_solta[df_solta['count'] < 25]

    check_and_attach(message_high, {f'solta_{tp}_high': df_solta_high, f'other_{tp}_high': df_other_high})
    check_and_attach(message_low, {f'solta_{tp}_low': df_solta_low, f'other_{tp}_low': df_other_low})


def main():
    os.mkdir(f'{settings.file_path}\\{today}')

    message_high = MIMEMultipart()
    message_high['From'] = settings.sender_email
    message_high['To'] = settings.receiver_email
    message_high['Subject'] = f'{email_subject}. Высокий приоритет'

    message_low = MIMEMultipart()
    message_low['From'] = settings.sender_email
    message_low['To'] = settings.receiver_email
    message_low['Subject'] = f'{email_subject}. Низкий приоритет'

    message_high.attach(MIMEText(message_high_text, 'html'))
    message_low.attach(MIMEText(message_low_text, 'html'))

    process_and_attach(message_high, message_low, report_web_file_url, 'web')
    process_and_attach(message_high, message_low, report_app_file_url, 'inapp')

    with smtplib.SMTP(settings.smtp_server, 587) as server:
        server.starttls()
        server.login(settings.sender_email, settings.sender_password)
        server.sendmail(settings.sender_email, settings.receiver_email, message_high.as_string())
        server.sendmail(settings.sender_email, settings.receiver_email, message_low.as_string())
        print(f'Email-ы за {today} отправлены')
    print('Нажмите что-то для завершения работы скрипта')
    input()


if __name__ == '__main__':
    if os.path.exists(f'{settings.file_path}/{today}'):
        print('Отчет уже сформирован и должен был быть отправлен. Если этого не произошло, повторите отправку вручную')
    else:
        main()
