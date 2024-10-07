import csv
import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta
import pandas as pd


def save_creatives(creative_dictionary_path, date, creatives):
    try:
        with open(creative_dictionary_path, 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        data = {}

    data[date] = list(creatives)

    with open(creative_dictionary_path, 'w') as file:
        json.dump(data, file)


def daily_statistics(creative_dictionary_path):
    try:
        with open(creative_dictionary_path, 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        logging.error("No previous data found.")
        return ''

    today = datetime.today()
    yesterday = (today - pd.Timedelta(days=1)).strftime('%Y-%m-%d') if today.weekday() != 0 \
        else (today - pd.Timedelta(days=3)).strftime('%Y-%m-%d')
    today = today.strftime('%Y-%m-%d')

    if today not in data or yesterday not in data:
        logging.warning("Not enough data for comparison.")
        return ''

    today_set = set(data[today])
    yesterday_set = set(data[yesterday])

    diff_creatives = today_set ^ yesterday_set
    repeated_creatives = today_set & yesterday_set

    data[f'{today}_diff_creatives'] = list(diff_creatives)
    data[f'{today}_repeated_creatives'] = list(repeated_creatives)

    with open(creative_dictionary_path, 'w') as file:
        json.dump(data, file)

    return f'\n\nComparing {today} to {yesterday}:\n' \
           f'{len(diff_creatives)} different crids, {len(repeated_creatives)} repeated crids'


def weekly_statistics(creative_dictionary_path, stuck_creatives_weekly_path):
    with open(creative_dictionary_path, 'r') as file:
        data = json.load(file)

    today = datetime.today().strftime('%Y-%m-%d')
    last_week_dates = [(datetime.today() - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]

    # Фильтруем данные только за последнюю неделю
    weekly_data = {date: set(data[date]) for date in last_week_dates if date in data}

    if len(weekly_data) < 2:
        logging.warning("Not enough data for weekly statistics.")
        return ''

    # Статистика за неделю
    total_new_creatives = 0
    total_repeated_creatives = 0
    new_creatives_per_day = []
    repeated_creatives_per_day = []
    stuck_creatives = set()

    all_creatives_week = set()  # Для хранения всех креативов за неделю
    appearances = defaultdict(int)

    # Пройдем по каждому дню недели и посчитаем нужную статистику
    for i in range(1, len(last_week_dates)):
        today_set = weekly_data.get(last_week_dates[i], set())
        yesterday_set = weekly_data.get(last_week_dates[i - 1], set())

        if not today_set:
            continue

        # Новые креативы и повторяющиеся
        new_creatives = today_set - yesterday_set
        repeated_creatives = today_set & yesterday_set

        # Обновляем общую статистику
        total_new_creatives += len(new_creatives)
        total_repeated_creatives += len(repeated_creatives)

        # Собираем статистику по дням
        new_creatives_per_day.append(len(new_creatives))
        repeated_creatives_per_day.append(len(repeated_creatives))

        # Накапливаем уникальные креативы за неделю
        all_creatives_week.update(today_set)

        # Увеличиваем счетчик появлений каждого креатива
        for creative in today_set:
            appearances[creative] += 1

        # "Зависшие" креативы — те, которые несколько дней подряд не проходят
        stuck_creatives.update(repeated_creatives)

    # Определяем креативы, которые появились несколько раз за неделю
    intersection_creatives_week = {creative for creative, count in appearances.items() if count > 1}
    print(intersection_creatives_week)

    # Сохраняем пересечение всех креативов за неделю в CSV
    with open(stuck_creatives_weekly_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Date', 'Creative'])
        for creative in intersection_creatives_week:
            writer.writerow([today, creative])

    # Рассчитываем статистику
    avg_new_creatives = sum(new_creatives_per_day) / len(new_creatives_per_day) if new_creatives_per_day else 0
    avg_repeated_creatives = sum(repeated_creatives_per_day) / len(
        repeated_creatives_per_day) if repeated_creatives_per_day else 0
    max_new_creatives = max(new_creatives_per_day, default=0)
    max_repeated_creatives = max(repeated_creatives_per_day, default=0)

    # Формируем вывод
    result = (
        f"Weekly Statistics ({last_week_dates[-1]} - {last_week_dates[0]}):\n"
        f"Total new creatives: {total_new_creatives}\n"
        f"Total repeated creatives: {total_repeated_creatives}\n"
        f"Average new creatives per day: {avg_new_creatives:.2f}\n"
        f"Average repeated creatives per day: {avg_repeated_creatives:.2f}\n"
        f"Max new creatives in a day: {max_new_creatives}\n"
        f"Max repeated creatives in a day: {max_repeated_creatives}\n"
        f"Stuck creatives (reappearing multiple days): {len(stuck_creatives)}\n{'-' * 50}\n\n"
    )

    logging.info(result)
    print(result)

    return result
