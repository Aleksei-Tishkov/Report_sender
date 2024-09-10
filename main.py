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
import logging

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

file_path = f'{settings.file_path}\\Reports\\'

message_high_text = settings.message_high_text
message_low_text = settings.message_low_text


logging.basicConfig(filename=f'{settings.file_path}//process_log.txt', level=logging.INFO,
                    format='%(asctime)s - %(message)s')

high_priority_count = 0
low_priority_count = 0


def check_and_attach(message, d):
    for filename, dataframe in d.items():
        if sum(dataframe.index) > 1:
            attachment = MIMEBase('application', 'octet-stream')
            dataframe.to_csv(f'{file_path}//{today}//{filename}_{today}.csv', index=False)
            csv_data = dataframe.to_csv(index=False)
            attachment.set_payload(csv_data.encode('utf-8'))
            encoders.encode_base64(attachment)
            attachment.add_header('Content-Disposition', f'attachment; filename="{filename}_{today}.csv"')
            message.attach(attachment)
            logging.info(f'{filename} formed')
        else:
            logging.info(f'{filename} is empty')
            print(f'C файлом {filename} что-то не так, проверьте')


def process_and_attach(message_high, message_low, path, tp):
    global high_priority_count, low_priority_count
    flag_high, flag_low = False, False
    for _ in range(5):
        response = requests.get(path)
        if response.status_code == 200:
            logging.info(f'{response.status_code}: successfully got csv on {path}')
            break
        else:
            logging.info(f'{response.status_code}: unavailable {path}')
    else:
        raise BaseException('Ошибка доступа к серверу с отчетами')

    df = pd.read_csv(StringIO(response.text))

    df_solta = df[df['dsp'] == 'solta'].iloc[:, 1:]
    df_other = df[df['dsp'] == 'other'].iloc[:, 1:]

    df_other_high = df_other[df_other['count'] >= 100]
    df_other_low = df_other[df_other['count'] < 100]
    high_priority_rows = len(df_other_high)
    logging.info(f"Other {tp} high-priority report contains {high_priority_rows} rows")
    high_priority_count += high_priority_rows
    low_priority_rows = len(df_other_low)
    logging.info(f"Other DSPs {tp} low-priority report contains {low_priority_rows} rows")
    low_priority_count += low_priority_rows
    if len(df_other_high):
        flag_high = True
    if len(df_other_low):
        flag_low = True

    df_solta_high = df_solta[df_solta['count'] >= 25]
    df_solta_low = df_solta[df_solta['count'] < 25]
    logging.info(f"Solta {tp} high-priority report contains {len(df_solta_high)} rows")
    logging.info(f"Solta DSPs {tp} low-priority report contains {len(df_solta_low)} rows")

    check_and_attach(message_high, {f'solta_{tp}_high': df_solta_high, f'other_{tp}_high': df_other_high})
    check_and_attach(message_low, {f'solta_{tp}_low': df_solta_low, f'other_{tp}_low': df_other_low})

    return flag_high, flag_low


def main():
    os.mkdir(f'{file_path}\\{today}')

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

    web_high_flag, web_low_flag = process_and_attach(message_high, message_low, report_web_file_url, 'web')
    app_high_flag, app_low_flag = process_and_attach(message_high, message_low, report_app_file_url, 'inapp')

    high_attachment_flag = web_high_flag or app_high_flag
    low_attachment_flag = web_low_flag or app_low_flag

    with smtplib.SMTP(settings.smtp_server, 587) as server:
        server.starttls()
        server.login(settings.sender_email, settings.sender_password)
        if high_attachment_flag:
            server.sendmail(settings.sender_email, settings.receiver_email, message_high.as_string())
            logging.info(f'High-priority e-mail sent with {high_priority_count} rows in total')
            print(f'High-priority e-mail за {today} отправлен, в нем {high_priority_count} строк в сумме.')
        else:
            logging.info(f'High-priority e-mail is NOT sent')
            print(f'High-priority e-mail за {today} не отправлен - отчеты пусты')
        if low_attachment_flag:
            server.sendmail(settings.sender_email, settings.receiver_email, message_low.as_string())
            logging.info(f'Low-priority e-mail sent  with {low_priority_count} rows in total')
            print(f'Low-priority e-mail за {today} отправлен, в нем {low_priority_count} строк в сумме')
        else:
            logging.info(f'Low-priority e-mail is NOT sent')
            print(f'Low-priority e-mail за {today} не отправлен - отчеты пусты')
    logging.info('script ended successfully')
    print('Нажмите что-то для завершения работы скрипта')
    input()


if __name__ == '__main__':
    if os.path.exists(f'{file_path}/{today}'):
        print('Отчет уже сформирован и должен был быть отправлен. Если этого не произошло, повторите отправку вручную')
    else:
        main()
