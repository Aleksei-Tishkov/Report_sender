import os

import pandas
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

import telegram
from telegram import Bot
import asyncio

import settings


def format_number(number):
    number = f"{number:,}".replace(",", " ")
    return number


async def update_log_files():
    df = pd.read_excel(excel_path)

    new_row = pd.DataFrame(excel_d, index=[0])
    df = pd.concat([df, new_row], ignore_index=True)

    df.to_excel(excel_path, index=False)
    logging.info(f"Updated excel_log.xlsx with new row for {today}")

    for k in excel_d.keys():
        if k == 'dt':
            continue
        excel_d[k] = format_number(excel_d[k])

    msg = f"{today}\n\nHIGH PRIORITY\n\n" \
          f"Solta WEB: {excel_d['Solta_web_high_domains']} domains, " \
          f"{excel_d['Solta_web_high_reqs']} requests\n" \
          f"Solta INAPP: {excel_d['Solta_inapp_high_domains']} domains, " \
          f"{excel_d['Solta_inapp_high_reqs']} requests\n\n" \
          f"Other WEB: {excel_d['Other_web_high_domains']} domains, " \
          f"{excel_d['Other_web_high_reqs']} requests\n" \
          f"Other INAPP: {excel_d['Other_inapp_high_domains']} domains, " \
          f"{excel_d['Other_inapp_high_reqs']} requests\n\nLOW PRIORITY\n\n" \
          f"Solta WEB: {excel_d['Solta_web_low_domains']} domains, " \
          f"{excel_d['Solta_web_low_reqs']} requests\n" \
          f"Solta INAPP: {excel_d['Solta_inapp_low_domains']} domains, " \
          f"{excel_d['Solta_inapp_low_reqs']} requests\n\n" \
          f"Other WEB: {excel_d['Other_web_low_domains']} domains, " \
          f"{excel_d['Other_web_low_reqs']} requests\n" \
          f"Other INAPP: {excel_d['Other_inapp_low_domains']} domains, " \
          f"{excel_d['Other_inapp_low_reqs']} requests"
    with open(txt_path, 'a') as file:
        file.write(msg + '\n\n\n')
    await bot.send_message(chat_id=settings.tg_recipient_id, text=msg)


def process_df(df: pandas.DataFrame):
    first_group = df.groupby(['adomain', 'dcid']).agg({
        'dcrid': lambda x: '\n'.join(map(str, x)),
        'count': 'sum'
    }).reset_index()

    second_group = first_group.groupby(['adomain']).agg({
        'dcid': lambda x: '\n'.join(sorted(set(map(str, x)))),
        'dcrid': lambda x: '\n'.join(sorted(set('\n'.join(x).split('\n')))),
        'count': 'sum'
    }).reset_index()

    result = second_group.sort_values(by='count', ascending=False)

    return result


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
            print(f'{filename} пуст')


def process_csv(message_high, message_low, path, tp):
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

    df.to_csv(f'{file_path}//_Reports_raw//{today}//raw_{tp}_report_{today}.csv', index=False)

    df_solta = df[df['dsp'] == 'solta'].iloc[:, 1:]

    df_other = df[df['dsp'] == 'other'].iloc[:, 1:]

    df_other_high = process_df(df_other[df_other['count'] >= 100])
    df_other_low = process_df(df_other[df_other['count'] < 100])

    rows = len(df_other_high)
    reqs = sum(df_other_high['count'])
    excel_d[f'Other_{tp}_high_domains'] = rows
    excel_d[f'Other_{tp}_high_reqs'] = reqs
    high_priority_count += rows
    logging.info(f"Other DSPs {tp} high-priority report contains {rows} rows, {reqs} requests")

    rows = len(df_other_low)
    reqs = sum(df_other_low['count'])
    excel_d[f'Other_{tp}_low_domains'] = rows
    excel_d[f'Other_{tp}_low_reqs'] = reqs
    low_priority_count += rows
    logging.info(f"Other DSPs {tp} low-priority report contains {rows} rows, {reqs} requests")

    df_solta_high = process_df(df_solta[df_solta['count'] >= 25])
    df_solta_low = process_df(df_solta[df_solta['count'] < 25])

    rows = len(df_solta_high)
    reqs = sum(df_solta_high['count'])
    excel_d[f'Solta_{tp}_high_domains'] = rows
    excel_d[f'Solta_{tp}_high_reqs'] = reqs
    high_priority_count += rows
    logging.info(f"Solta {tp} high-priority report contains {rows} rows, {reqs} requests")

    rows = len(df_solta_low)
    reqs = sum(df_solta_low['count'])
    excel_d[f'Solta_{tp}_low_domains'] = rows
    excel_d[f'Solta_{tp}_low_reqs'] = reqs
    low_priority_count += rows
    logging.info(f"Solta {tp} low-priority report contains {rows} rows, {reqs} requests")

    check_and_attach(message_high, {f'solta_{tp}_high': df_solta_high, f'other_{tp}_high': df_other_high})
    check_and_attach(message_low, {f'solta_{tp}_low': df_solta_low, f'other_{tp}_low': df_other_low})

    if len(df_other_high) or len(df_solta_high):
        flag_high = True
    if len(df_other_low) or len(df_solta_low):
        flag_low = True

    return flag_high, flag_low


def main():
    if os.path.exists(f'{file_path}/{today}'):
        input('Отчет уже сформирован и должен был быть отправлен. Если этого не произошло, повторите отправку вручную')
        return
    os.mkdir(f'{file_path}\\{today}')
    os.mkdir(f'{file_path}_Reports_raw\\{today}')

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

    web_high_flag, web_low_flag = process_csv(message_high, message_low, report_web_file_url, 'web')
    app_high_flag, app_low_flag = process_csv(message_high, message_low, report_app_file_url, 'inapp')

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
    asyncio.run(update_log_files())
    logging.info('Process finished successfully' + '-' * 50)
    input('Нажмите что-то для завершения работы скрипта')


today = date.today()

if today.weekday() == 0:
    report_web_file_url = settings.report_web_file_weekends_url
    report_app_file_url = settings.report_app_file_weekends_url
elif today.weekday() in (5, 6):
    input('Сегодня выходной, по выходным мы отчеты не отправляем')


    def main():
        pass
else:
    report_web_file_url = settings.report_web_file_weekdays_url
    report_app_file_url = settings.report_app_file_weekdays_url

today = today.strftime("%d.%m.%Y")

email_subject = f'{settings.email_subject}{today}'

log_path = settings.file_path
file_path = f'{settings.file_path}\\Reports\\'

message_high_text = settings.message_high_text
message_low_text = settings.message_low_text

logging.basicConfig(filename=os.path.join(log_path, 'process_log.txt'), level=logging.INFO,
                    format='%(asctime)s - %(message)s')

excel_path = os.path.join(log_path, 'excel_log.xlsx')
txt_path = os.path.join(log_path, 'txt_log.txt')
bot = Bot(token=settings.bot_token)

high_priority_count = 0
low_priority_count = 0

excel_d = {
    'dt': today, 'Solta_web_high_domains': 0, 'Solta_inapp_high_domains': 0,
    'Other_web_high_domains': 0, 'Other_inapp_high_domains': 0,
    'Solta_web_low_domains': 0, 'Solta_inapp_low_domains': 0,
    'Other_web_low_domains': 0, 'Other_inapp_low_domains': 0,
    'Solta_web_high_reqs': 0, 'Solta_inapp_high_reqs': 0,
    'Other_web_high_reqs': 0, 'Other_inapp_high_reqs': 0,
    'Solta_web_low_reqs': 0, 'Solta_inapp_low_reqs': 0,
    'Other_web_low_reqs': 0, 'Other_inapp_low_reqs': 0
}

if __name__ == '__main__':
    main()
