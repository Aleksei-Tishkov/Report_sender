import json
import logging
import os
from datetime import datetime

import pandas as pd

from settings import file_path


json_path = os.path.join(file_path, 'creative_dictionary.json')


def save_creatives(date, creatives):
    try:
        with open(json_path, 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        data = {}

    data[date] = list(creatives)

    with open(json_path, 'w') as file:
        json.dump(data, file)


def daily_statistics():
    try:
        with open(json_path, 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        logging.error("No previous data found.")
        return ''

    today = datetime.today().strftime('%d.%m.%Y')
    yesterday = (datetime.today() - pd.Timedelta(days=1)).strftime('%d.%m.%Y')

    if today not in data or yesterday not in data:
        logging.warning("Not enough data for comparison.")
        return ''

    today_set = set(data[today])
    yesterday_set = set(data[yesterday])

    diff_creatives = today_set ^ yesterday_set
    repeated_creatives = today_set & yesterday_set

    data[f'{today}_diff_creatives'] = list(diff_creatives)
    data[f'{today}_repeated_creatives'] = list(repeated_creatives)

    with open(json_path, 'w') as file:
        json.dump(data, file)

    return f'\n\nComparing {today} to {yesterday}:\n' \
           f'{len(diff_creatives)} different crids, {len(repeated_creatives)} repeated crids '


def weekly_statistics():
    try:
        with open(json_path, 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        logging.error("No data found.")
        return None

    today = datetime.today().strftime('%d.%m.%Y')
    last_week_dates = [(datetime.today() - timedelta(days=i)).strftime('%d.%m.%Y') for i in range(7)]

    # Фильтруем данные только за последнюю неделю
    weekly_data = {date: set(data[date]) for date in last_week_dates if date in data}

    if len(weekly_data) < 2:
        logging.warning("Not enough data for weekly statistics.")
        return ''

    total_new_creatives = 0
    total_repeated_creatives = 0
    new_creatives_per_day = []
    repeated_creatives_per_day = []
    stuck_creatives = set()

    all_creatives_week = set()
    repeated_creatives_week = weekly_data.get(last_week_dates[today], set())

    for i in range(1, len(last_week_dates)):
        today_set = weekly_data.get(last_week_dates[i], set())
        yesterday_set = weekly_data.get(last_week_dates[i-1], set())
        if not today_set:
            continue

        # Новые креативы и повторяющиеся
        new_creatives = today_set - yesterday_set
        repeated_creatives = today_set & yesterday_set
        repeated_creatives_week &= today_set

        # Обновляем общую статистику
        total_new_creatives += len(new_creatives)
        total_repeated_creatives += len(repeated_creatives)

        # Собираем статистику по дням
        new_creatives_per_day.append(len(new_creatives))
        repeated_creatives_per_day.append(len(repeated_creatives))

        # Накапливаем уникальные креативы за неделю
        all_creatives_week.update(today_set)

        # "Зависшие" креативы — те, которые несколько дней подряд не проходят
        stuck_creatives.update(repeated_creatives)

    # Рассчитываем статистику
    avg_new_creatives = sum(new_creatives_per_day) / len(new_creatives_per_day)
    avg_repeated_creatives = sum(repeated_creatives_per_day) / len(repeated_creatives_per_day)

    max_new_creatives = max(new_creatives_per_day, default=0)
    max_repeated_creatives = max(repeated_creatives_per_day, default=0)

    # Формируем вывод
    result = (
        f"Weekly Statistics ({last_week_dates[-1]} - {last_week_dates[0]}):\n\n"
        f"Total new creatives: {total_new_creatives}\n"
        f"Total repeated creatives: {total_repeated_creatives}\n"
        f"Average new creatives per day: {avg_new_creatives:.2f}\n"
        f"Average repeated creatives per day: {avg_repeated_creatives:.2f}\n"
        f"Max new creatives in a day: {max_new_creatives}\n"
        f"Max repeated creatives in a day: {max_repeated_creatives}\n"
        f"Stuck creatives (reappearing multiple days): {len(stuck_creatives)}\n"
    )

    data[f'Weekly Statistics ({last_week_dates[-1]} - {last_week_dates[0]})'] = list(repeated_creatives_week)

    with open(json_path, 'w') as file:
        json.dump(data, file)

    logging.info(result)
    return result
