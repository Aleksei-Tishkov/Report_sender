import os
import pandas as pd
from openpyxl import Workbook
from datetime import date

from settings import file_path


MAX_EXCEL_STR_LEN = 32767

today = date.today().strftime("%Y-%m-%d")

path = os.path.join(file_path, f'Reports/!Date_reports/{today}')


def get_csv_files(path):
    return [f for f in os.listdir(path) if f.endswith('.csv')]


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

    result = []

    for file in csv_files:
        df = pd.read_csv(os.path.join(path, file))

        while True:
            try:
                num_rows = int(input(f"Сколько строк обработать из файла {file}? (Максимум {len(df)} строк): "))
                if 0 < num_rows <= len(df):
                    break
                else:
                    print(f"Введите число от 1 до {len(df)}.")
            except ValueError:
                print("Введите корректное число.")

        df = df.head(num_rows)

        if 'dcrid' in df.columns:
            for idx, row in df.iterrows():
                dcrid_values = str(row['dcrid']).split('\n')
                result.extend(dcrid_values)
    write_to_excel(result, os.path.join(path, f'dcrid_data_{today}.xlsx'))

    input('Нажмите что-то для завершения работы скрипта')
    return
