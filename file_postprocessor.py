import json
import os
import pandas as pd
from openpyxl import Workbook
from datetime import date, timedelta

import settings

MAX_EXCEL_STR_LEN = 32767

today = date.today().strftime("%Y-%m-%d")

path = os.path.join(settings.file_path, f'Reports/{today}')
p_path = os.path.join(settings.file_path, f'Reports/')

json_processed_path = os.path.join(settings.file_path, 'processed.json')


def get_csv_files(path):
    return [f for f in os.listdir(path) if f.endswith('.csv') and f != 'unmoderated.csv']


def write_to_excel(data, output_file):
    wb = Workbook()
    ws = wb.active
    ws.title = "DCRID Data"
    for dcrid in data:
        ws.append([today, dcrid])
    wb.save(output_file)


def process_files(message_text):
    """
        Функция для обработки файлов на основе последнего сообщения из чата.
        Теперь работает в автоматическом режиме без ручного ввода.
        """
    import re
    import os
    import json
    import pandas as pd

    if not message_text or not message_text.startswith("Сегодня отправила на модерацию"):
        print("Не найдено подходящее сообщение для обработки")
        return 0

    # Извлекаем информацию о файлах и количестве строк
    file_pattern = r'([A-Za-z]+ (?:web|inapp) (?:high|low)) \((\d+)\)'
    file_matches = re.findall(file_pattern, message_text, re.IGNORECASE)

    if not file_matches:
        print("Не удалось найти информацию о файлах в сообщении")
        return 0

    # Словарь соответствия названий в сообщении и реальных имен файлов
    file_name_mapping = {
        'solta web high': f'solta_web_high_{today}.csv',
        'solta inapp high': f'solta_inapp_high_{today}.csv',
        'other web high': f'other_web_high_{today}.csv',
        'other inapp high': f'other_inapp_high_{today}.csv',
        'solta web low': f'solta_web_low_{today}.csv',
        'solta inapp low': f'solta_inapp_low_{today}.csv',
        'other web low': f'other_web_low_{today}.csv',
        'other inapp low': f'other_inapp_low_{today}.csv'
    }

    # Получаем список CSV файлов
    def get_csv_files(directory):
        if not os.path.exists(directory):
            return []
        return [f for f in os.listdir(directory) if f.endswith('.csv')]

    csv_files = get_csv_files(path)
    if not csv_files:
        print(f'В папке {today} нет исходных файлов.')
        return 0

    result_crids, result_variants = [], []

    # Обрабатываем каждый файл, указанный в сообщении
    for file_desc, num_rows_str in file_matches:
        print(file_desc, num_rows_str)
        file_desc_lower = file_desc.lower()
        file_name = file_name_mapping.get(file_desc_lower)

        if not file_name or file_name not in csv_files:
            print(f"Файл {file_desc} не найден")
            continue

        try:
            num_rows = int(num_rows_str)
            if num_rows <= 0:
                continue

            df = pd.read_csv(os.path.join(path, file_name))

            # Проверяем, что количество строк не превышает размер файла
            if num_rows > len(df):
                num_rows = len(df)

            df = df.head(num_rows)

            if 'dcrid' in df.columns:
                for idx, row in df.iterrows():
                    dcrid_values = str(row['dcrid']).split('\n')
                    variant_values = str(row['variant_id']).split(', \n')
                    result_crids.extend(dcrid_values)
                    for v in variant_values:
                        v = v.split(', ')
                        result_variants.extend(v)
        except Exception as e:
            print(f"Ошибка при обработке файла {file_name}: {e}")

    # Записываем результаты в Excel
    write_to_excel(result_crids, os.path.join(p_path, f'!Date_reports/dcrid_data_{today}.xlsx'))

    # Сохраняем данные в JSON
    try:
        with open(json_processed_path, 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        data = {}

    data[today] = {'crids': result_crids, 'variants': result_variants}

    with open(json_processed_path, 'w') as file:
        json.dump(data, file)

    return len(result_crids)


def get_previous_working_day(today):
    previous_day = today - timedelta(days=1)
    while previous_day.weekday() >= 5:  # Пропускаем выходные (сб, вс)
        previous_day -= timedelta(days=1)
    return previous_day.strftime('%Y-%m-%d')


def get_stuck_crids_from_json(today, json_path):
    with open(json_path, 'r') as f:
        crid_data = json.load(f)

    # Получаем даты за предыдущую неделю (учитывая рабочие дни)
    previous_week_dates = []
    current_date = today - timedelta(days=1)
    previous_working_day = get_previous_working_day(today)

    while len(previous_week_dates) < 5:
        if current_date.weekday() < 5:  # Пн-Пт
            previous_week_dates.append(current_date.strftime('%Y-%m-%d'))
        current_date -= timedelta(days=1)

    # Собираем все crid за эту неделю
    crid_weekly_sets = []
    for date in previous_week_dates:
        if date in crid_data:
            crid_weekly_sets.extend(crid_data[date]['crids'])

    previous_day_crids = set()
    if previous_working_day in crid_data:
        previous_day_crids = set(crid_data[previous_working_day]['crids'])

    # Находим crid-ы, которые встречаются более одного раза
    crid_counts = pd.Series(crid_weekly_sets).value_counts()
    duplicate_crids = set(crid_counts[crid_counts > 1].index)

    return duplicate_crids, previous_day_crids
