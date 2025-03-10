import concurrent
import json
import os

import pandas
import requests as requests
from datetime import date, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import pandas as pd
from io import BytesIO
import logging

from telegram import Bot
import asyncio

from telegram.error import TimedOut

import file_postprocessor

import settings
import stats


def format_number(number):
    number = f"{number:,}".replace(",", " ")
    return number


def pluralize(word, count):
    return f"{word}{'' if count == 1 else 's'}"


async def send_message_with_retry(chat_id, text):
    try:
        await bot.send_message(chat_id=chat_id, text=text)
    except TimedOut:
        print(f'Error: TimedOut. Message could not be sent: \n{text}')


def format_message(excel_d, today):
    msg = f"{today}\n\n"
    high_priority = []
    if excel_d.get('Solta_web_high_domains') != '0':
        domains = excel_d['Solta_web_high_domains']
        crids = excel_d['Solta_web_high_crids']
        requests = excel_d['Solta_web_high_reqs']
        high_priority.append(f"Solta WEB: {domains} {pluralize('adomain', domains)}, "
                             f"{crids} {pluralize('crid', crids)}, "
                             f"{requests} {pluralize('request', requests)}")
    if excel_d.get('Solta_inapp_high_domains') != '0':
        domains = excel_d['Solta_inapp_high_domains']
        crids = excel_d['Solta_inapp_high_crids']
        requests = excel_d['Solta_inapp_high_reqs']
        high_priority.append(f"Solta INAPP: {domains} {pluralize('adomain', domains)}, "
                             f"{crids} {pluralize('crid', crids)}, "
                             f"{requests} {pluralize('request', requests)}")
    if excel_d.get('Other_web_high_domains') != '0':
        domains = excel_d['Other_web_high_domains']
        crids = excel_d['Other_web_high_crids']
        requests = excel_d['Other_web_high_reqs']
        high_priority.append(f"Other WEB: {domains} {pluralize('adomain', domains)}, "
                             f"{crids} {pluralize('crid', crids)}, "
                             f"{requests} {pluralize('request', requests)}")
    if excel_d.get('Other_inapp_high_domains') != '0':
        domains = excel_d['Other_inapp_high_domains']
        crids = excel_d['Other_inapp_high_crids']
        requests = excel_d['Other_inapp_high_reqs']
        high_priority.append(f"Other INAPP: {domains} {pluralize('adomain', domains)}, "
                             f"{crids} {pluralize('crid', crids)}, "
                             f"{requests} {pluralize('request', requests)}")

    if high_priority:
        msg += "HIGH PRIORITY\n\n" + "\n".join(high_priority) + "\n\n"
    else:
        msg += "HIGH PRIORITY: No data available\n\n"

    low_priority = []
    if excel_d.get('Solta_web_low_domains') != '0':
        domains = excel_d['Solta_web_low_domains']
        crids = excel_d['Solta_web_low_crids']
        requests = excel_d['Solta_web_low_reqs']
        low_priority.append(f"Solta WEB: {domains} {pluralize('adomain', domains)}, "
                            f"{crids} {pluralize('crid', crids)}, "
                            f"{requests} {pluralize('request', requests)}")
    if excel_d.get('Solta_inapp_low_domains') != '0':
        domains = excel_d['Solta_inapp_low_domains']
        crids = excel_d['Solta_inapp_low_crids']
        requests = excel_d['Solta_inapp_low_reqs']
        low_priority.append(f"Solta INAPP: {domains} {pluralize('adomain', domains)}, "
                            f"{crids} {pluralize('crid', crids)}, "
                            f"{requests} {pluralize('request', requests)}")
    if excel_d.get('Other_web_low_domains') != '0':
        domains = excel_d['Other_web_low_domains']
        crids = excel_d['Other_web_low_crids']
        requests = excel_d['Other_web_low_reqs']
        low_priority.append(f"Other WEB: {domains} {pluralize('adomain', domains)}, "
                            f"{crids} {pluralize('crid', crids)}, "
                            f"{requests} {pluralize('request', requests)}")
    if excel_d.get('Other_inapp_low_domains') != '0':
        domains = excel_d['Other_inapp_low_domains']
        crids = excel_d['Other_inapp_low_crids']
        requests = excel_d['Other_inapp_low_reqs']
        low_priority.append(f"Other INAPP: {domains} {pluralize('adomain', domains)}, "
                            f"{crids} {pluralize('crid', crids)}, "
                            f"{requests} {pluralize('request', requests)}")

    if low_priority:
        msg += "LOW PRIORITY\n\n" + "\n".join(low_priority)
    else:
        msg += "LOW PRIORITY: No data available"

    msg += f'\n\nUnmoderated crids: {excel_d["unmoderated"]}'

    # msg += stats.daily_statistics(creative_dictionary_path) # think over - what daily stats should be here/

    return msg


async def update_log_files():
    df = pd.read_excel(excel_path)

    new_row = pd.DataFrame(excel_d, index=[0])
    df = pd.concat([df, new_row], ignore_index=True)

    df.to_excel(excel_path, index=False)
    logging.info(f"Updated excel_log.xlsx with new row for {today_str}")

    for k in excel_d.keys():
        if k == 'dt':
            continue
        excel_d[k] = format_number(excel_d[k])

    msg = format_message(excel_d, today_str)

    with open(txt_path, 'a') as file:
        file.write(msg + '\n\n\n')

    await send_message_with_retry(chat_id=settings.tg_recipient_id, text=msg)

    if date.today().weekday() == 0:
        msg = stats.weekly_statistics(creative_dictionary_path,
                                      stuck_creatives_weekly_path,
                                      today_creatives,
                                      settings.file_path)
        with open(txt_path, 'a') as file:
            file.write(msg + '\n\n\n')
        await send_message_with_retry(chat_id=settings.tg_recipient_id, text=msg)


def process_df(df: pandas.DataFrame):
    df = df.copy()

    df['variant_id'] = df['variant_id'].astype(str)  # Явно переводим variant_id в строку
    df['dcrid'] = df['dcrid'].astype(str)  # Убеждаемся, что dcrid тоже строка

    # Формируем новый variant_id
    df.loc[:, 'variant_id'] = df['dcrid'].str.split('.').str[0] + '.' + df['variant_id']
    today_variants.update(df['variant_id'].unique())

    first_group = df.groupby(['adomain', 'dcid']).agg({
        'dcrid': lambda x: '\n'.join(map(str, x)),
        'count': 'sum',
        'variant_id': lambda x: ', '.join(map(str, set(x)))  # Добавляем variant_id
    }).reset_index()

    second_group = first_group.groupby(['adomain']).agg({
        'dcid': lambda x: '\n'.join(sorted(set(map(str, x)))),
        'dcrid': lambda x: '\n'.join(sorted(set('\n'.join(map(str, x)).split('\n')))),
        'count': 'sum',
        'variant_id': lambda x: ', '.join(sorted(set(', '.join(map(str, x)).split(', '))))  # Учитываем variant_id
    }).reset_index()

    result = second_group.sort_values(by='count', ascending=False)

    return result


def count_crid(df):
    df['dcrid'] = df['dcrid'].fillna('').astype(str)
    crid_counts = df['dcrid'].fillna('').str.split('\n').str.len()
    return int(crid_counts.sum())


def check_and_attach(message, d):
    for filename, dataframe in d.items():
        if len(dataframe.index) > 0:
            attachment = MIMEBase('application', 'octet-stream')
            dataframe.to_csv(f'{file_path}//{today_str}//{filename}_{today_str}.csv', index=False)
            csv_data = dataframe.to_csv(index=False)
            attachment.set_payload(csv_data.encode('utf-8'))
            encoders.encode_base64(attachment)
            attachment.add_header('Content-Disposition', f'attachment; filename="{filename}_{today_str}.csv"')
            message.attach(attachment)
            logging.info(f'{filename} formed')
        else:
            logging.info(f'{filename} is empty')
            print(f'{filename} пуст')


async def process_csv(message_high, message_low, path, tp):
    global high_priority_count, low_priority_count
    flag_high, flag_low = False, False

    for _ in range(5):
        response = requests.get(path)
        if response.status_code == 200:
            logging.info(f'{response.status_code}: successfully got {tp} csv')
            break
        else:
            logging.info(f'{response.status_code}: unavailable {path}')
    else:
        raise BaseException('Ошибка доступа к серверу с отчетами')

    df = pd.read_csv(BytesIO(response.content))

    yesterday = (today - pd.Timedelta(days=1)).strftime('%Y-%m-%d') if today.weekday() != 0 \
        else (today - pd.Timedelta(days=3)).strftime('%Y-%m-%d')

    with open(file_postprocessor.json_processed_path, 'r') as file:
        json_data = json.load(file)

    crid_list = json_data.get(yesterday, []) if json_data else []

    df = df[~df['dcrid'].isin(crid_list)]

    today_creatives.update(df['dcrid'].unique())

    df.to_csv(f'{file_path}//_Reports_raw//{today_str}//raw_{tp}_report_{today_str}.csv', index=False)

    # duplicate_crids, previous_day_crids = file_postprocessor.get_stuck_crids_from_json(date.today_str(), creative_dictionary_path)
    # crids_to_exclude = duplicate_crids.union(previous_day_crids)

    df_solta = df[df['dsp'] == 'solta'].iloc[:, 1:]
    # df_solta = df_solta[~df_solta['dcrid'].isin(crids_to_exclude)]

    df_other = df[df['dsp'] == 'other'].iloc[:, 1:]
    # df_other = df_other[~df_other['dcrid'].isin(crids_to_exclude)]

    df_other_high = process_df(df_other[df_other['count'] >= 100])
    df_other_low = process_df(df_other[df_other['count'] < 100])

    rows = len(df_other_high)
    crids = count_crid(df_other_high)
    reqs = sum(df_other_high['count'])
    excel_d[f'Other_{tp}_high_domains'] = rows
    excel_d[f'Other_{tp}_high_crids'] = crids
    excel_d[f'Other_{tp}_high_reqs'] = reqs
    high_priority_count += rows
    logging.info(f"Other DSPs {tp} high-priority report contains {rows} rows, {crids} crids, {reqs} requests")

    rows = len(df_other_low)
    crids = count_crid(df_other_low)
    reqs = sum(df_other_low['count'])
    excel_d[f'Other_{tp}_low_domains'] = rows
    excel_d[f'Other_{tp}_low_crids'] = crids
    excel_d[f'Other_{tp}_low_reqs'] = reqs
    low_priority_count += rows
    logging.info(f"Other DSPs {tp} low-priority report contains {rows} rows, {crids} crids, {reqs} requests")

    df_solta_high = process_df(df_solta[df_solta['count'] >= 25])
    df_solta_low = process_df(df_solta[df_solta['count'] < 25])

    rows = len(df_solta_high)
    crids = count_crid(df_solta_high)
    reqs = sum(df_solta_high['count'])
    excel_d[f'Solta_{tp}_high_domains'] = rows
    excel_d[f'Solta_{tp}_high_crids'] = crids
    excel_d[f'Solta_{tp}_high_reqs'] = reqs
    high_priority_count += rows
    logging.info(f"Solta {tp} high-priority report contains {rows} rows, {crids} crids, {reqs} requests")

    rows = len(df_solta_low)
    crids = count_crid(df_solta_low)
    reqs = sum(df_solta_low['count'])
    excel_d[f'Solta_{tp}_low_domains'] = rows
    excel_d[f'Solta_{tp}_low_crids'] = crids
    excel_d[f'Solta_{tp}_low_reqs'] = reqs
    low_priority_count += rows
    logging.info(f"Solta {tp} low-priority report contains {rows} rows, {crids} crids, {reqs} requests")

    check_and_attach(message_high, {f'solta_{tp}_high': df_solta_high, f'other_{tp}_high': df_other_high})
    check_and_attach(message_low, {f'solta_{tp}_low': df_solta_low, f'other_{tp}_low': df_other_low})

    if len(df_other_high) or len(df_solta_high):
        flag_high = True
    if len(df_other_low) or len(df_solta_low):
        flag_low = True

    return flag_high, flag_low


async def process_unmoderated():
    df = pd.read_csv(settings.report_unmoderated)
    df.to_csv(f'{file_path}{today_str}//unmoderated.csv', index=False)
    excel_d["unmoderated"] = len(df)


async def send_email(server, email, message):
    await asyncio.sleep(0)
    server.sendmail(settings.sender_email, email, message.as_string())


async def get_moderation_message_from_chat(chat_id=settings.tg_recipient_id):
    """
    Асинхронная функция для получения последнего сообщения о модерации
    с использованием объекта Bot.
    """
    while True:
        try:
            updates = await bot.get_updates(limit=5)
            for update in reversed(updates):
                if (hasattr(update, 'message') and update.message and
                        update.message.date.date() == date.today() and
                        update.message.chat.id == int(chat_id) and
                        update.message.text and
                        update.message.text.startswith("Сегодня отправила на модерацию")):
                    return update.message.text
        except Exception as e:
            print(f"Ошибка при получении сообщений из чата: {e}")

        await asyncio.sleep(60)


async def post_process_files():
    """
    Asynchronous function to process files and send messages.
    Now waits for the moderation message before processing.
    """
    import asyncio
    import concurrent.futures

    # Wait for moderation message with more attempts and longer timeout
    moderation_message = await get_moderation_message_from_chat()
    print(moderation_message)

    loop = asyncio.get_event_loop()

    # Define a wrapper function that takes the message as input
    def process_files_wrapper(message_text):
        return file_postprocessor.process_files(message_text)

    with concurrent.futures.ThreadPoolExecutor() as pool:
        # Use the wrapper function and pass the message as an argument
        crid_quantity = await loop.run_in_executor(
            pool,
            process_files_wrapper,
            moderation_message
        )

    if crid_quantity > 0:
        msg = f'{today_str}\n\n{pluralize("Crid", crid_quantity)} sent on moderation: {crid_quantity}'
        await send_message_with_retry(chat_id=settings.tg_recipient_id, text=msg)
    else:
        msg = f'{today_str}\n\nНе удалось обработать файлы. Проверьте наличие файлов и формат сообщения.'
        await send_message_with_retry(chat_id=settings.tg_recipient_id, text=msg)

    print('Обработка завершена')
    input('Нажмите что-то для завершения работы скрипта')


# This is what's in the original code:
# async def post_process_files():
#     """
#         Асинхронная функция для обработки файлов и отправки сообщения.
#         Сохраняет прежнее название, но теперь работает без ручного ввода.
#         """
#     import asyncio
#     import concurrent.futures
#     from datetime import datetime
#
#     today_str = datetime.now().strftime('%d.%m.%Y')
#
#     loop = asyncio.get_event_loop()
#
#     with concurrent.futures.ThreadPoolExecutor() as pool:
#         crid_quantity = await loop.run_in_executor(pool, file_postprocessor.process_files, get_moderation_message_from_chat)
#
#     if crid_quantity > 0:
#         msg = f'{today_str}\n\n{pluralize("Crid", crid_quantity)} sent on moderation: {crid_quantity}'
#         await send_message_with_retry(chat_id=settings.tg_recipient_id, text=msg)
#     else:
#         msg = f'{today_str}\n\nНе удалось обработать файлы. Проверьте наличие файлов и формат сообщения.'
#         await send_message_with_retry(chat_id=settings.tg_recipient_id, text=msg)
#
#     print('Обработка завершена')
#     input('Нажмите что-то для завершения работы скрипта')


def setup_message_handler():
    """
    Sets up a message handler for the bot.
    Continuously checks for messages with appropriate pattern.
    """
    import asyncio
    import threading

    async def check_messages():
        """
        Asynchronous function to check for new messages.
        """
        last_update_id = 0
        print("Starting to monitor for moderation messages...")

        while True:
            try:
                # Get updates since the last received ID
                updates = await bot.get_updates(offset=last_update_id + 1, timeout=120)

                for update in updates:
                    last_update_id = update.update_id

                    # Check messages
                    if hasattr(update, 'message') and update.message and update.message.text:
                        if update.message.text.startswith("Сегодня отправила на модерацию"):
                            # Reply to chat about starting processing

                            # Start file processing
                            await post_process_files()

                            # Send completion message

            except Exception as e:
                print(f"Error checking messages: {e}")

            # Wait a short time before next check
            await asyncio.sleep(5)

    # Start message checking in a separate thread
    def run_async_loop():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(check_messages())

    thread = threading.Thread(target=run_async_loop, daemon=True)
    thread.start()

    print("Message handler for automatic file processing is running")
    return thread


async def main():
    if os.path.exists(f'{file_path}{today_str}'):
        input('Отчет уже сформирован и должен был быть отправлен. Если этого не произошло, повторите отправку вручную')
        await post_process_files()
        return
    os.makedirs(f'{file_path}\\{today_str}', exist_ok=True)
    os.makedirs(f'{file_path}_Reports_raw\\{today_str}', exist_ok=True)
    os.makedirs(f'{file_path}!Date_reports\\{today_str}', exist_ok=True)

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

    web_high_flag, web_low_flag = await process_csv(message_high, message_low, report_web_file_url, 'web')
    app_high_flag, app_low_flag = await process_csv(message_high, message_low, report_app_file_url, 'inapp')
    await process_unmoderated()

    high_attachment_flag = web_high_flag or app_high_flag
    low_attachment_flag = web_low_flag or app_low_flag

    with smtplib.SMTP(settings.smtp_server, 587) as server:
        server.starttls()
        server.login(settings.sender_email, settings.sender_password)
        if high_attachment_flag:
            await send_email(server, settings.receiver_email, message_high)
            logging.info(f'High-priority e-mail sent with {high_priority_count} rows in total')
            print(f'High-priority e-mail за {today_str} отправлен, в нем {high_priority_count} строк в сумме.')
        else:
            logging.info(f'High-priority e-mail is NOT sent')
            print(f'High-priority e-mail за {today_str} не отправлен - отчеты пусты')
        if low_attachment_flag:
            await send_email(server, settings.receiver_email, message_low)
            logging.info(f'Low-priority e-mail sent  with {low_priority_count} rows in total')
            print(f'Low-priority e-mail за {today_str} отправлен, в нем {low_priority_count} строк в сумме')
        else:
            logging.info(f'Low-priority e-mail is NOT sent')
            print(f'Low-priority e-mail за {today_str} не отправлен - отчеты пусты')
    stats.save_creatives(creative_dictionary_path, today_str, today_creatives, today_variants)
    await update_log_files()
    logging.info('Process finished successfully' + '-' * 50 + '\n')
    await post_process_files()


today = date.today()

if today.weekday() == 0:
    report_web_file_url = settings.report_web_file_weekends_url
    report_app_file_url = settings.report_app_file_weekends_url
elif today.weekday() in (1, ):
    input('Сегодня выходной, по выходным мы отчеты не отправляем')

    def main():
        pass
else:
    report_web_file_url = settings.report_web_file_weekdays_url
    report_app_file_url = settings.report_app_file_weekdays_url

today_str = today.strftime("%Y-%m-%d")

# today_str = today_str.strftime("%d.%m.%Y")

email_subject = f'{settings.email_subject}{today_str}'

log_path = settings.file_path

logging.basicConfig(filename=os.path.join(log_path, 'process_log.txt'), level=logging.INFO,
                    format='%(asctime)s - %(message)s')

logging.getLogger('telegram').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)

file_path = f'{settings.file_path}\\Reports\\'

creative_dictionary_path = os.path.join(settings.file_path, 'creative_dictionary.json')
stuck_creatives_weekly_path = os.path.join(settings.file_path, 'stuck_creatives_weekly.csv')
excel_path = os.path.join(log_path, 'excel_log.xlsx')
txt_path = os.path.join(log_path, 'txt_log.txt')

message_high_text = settings.message_high_text
message_low_text = settings.message_low_text

bot = Bot(token=settings.bot_token)

high_priority_count = 0
low_priority_count = 0

excel_d = {
    'dt': today_str, 'Solta_web_high_domains': 0, 'Solta_inapp_high_domains': 0,
    'Other_web_high_domains': 0, 'Other_inapp_high_domains': 0,
    'Solta_web_low_domains': 0, 'Solta_inapp_low_domains': 0,
    'Other_web_low_domains': 0, 'Other_inapp_low_domains': 0,
    'Solta_web_high_crids': 0, 'Solta_inapp_high_crids': 0,
    'Other_web_high_crids': 0, 'Other_inapp_high_crids': 0,
    'Solta_web_low_crids': 0, 'Solta_inapp_low_crids': 0,
    'Other_web_low_crids': 0, 'Other_inapp_low_crids': 0,
    'Solta_web_high_reqs': 0, 'Solta_inapp_high_reqs': 0,
    'Other_web_high_reqs': 0, 'Other_inapp_high_reqs': 0,
    'Solta_web_low_reqs': 0, 'Solta_inapp_low_reqs': 0,
    'Other_web_low_reqs': 0, 'Other_inapp_low_reqs': 0,
    'unmoderated': 0
}

today_creatives, today_variants = set(), set()


if __name__ == '__main__':
    #try:
    asyncio.run(main())  # Запускаем событийный цикл только один раз
    #except Exception as e:
        #input(f'Process terminated with Exception: {e}')
        #logging.exception(e)
