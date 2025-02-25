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


def process_files():
    while True:
        csv_files = get_csv_files(path)
        if not csv_files:
            input(f'В папке {today} нет исходных файлов. Убедитесь в их наличии и нажмите что-нибудь')
            continue
        else:
            break

    result_crids, result_variants = [], []

    for file in csv_files:
        df = pd.read_csv(os.path.join(path, file))

        while True:
            try:
                num_rows = int(input(f"Сколько строк обработать из файла {file}? (Максимум {len(df)} строк): "))
                if 0 <= num_rows <= len(df):
                    break
                else:
                    print(f"Введите число от 1 до {len(df)}.")
            except ValueError:
                print("Введите корректное число.")

        df = df.head(num_rows)

        if 'dcrid' in df.columns:
            for idx, row in df.iterrows():
                dcrid_values = str(row['dcrid']).split('\n')
                variant_values = str(row['variant_id']).split(', \n')
                result_crids.extend(dcrid_values)
                for v in variant_values:
                    v = v.split(', ')
                    result_variants.extend(v)

    write_to_excel(result_crids, os.path.join(p_path, f'!Date_reports/dcrid_data_{today}.xlsx'))
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
