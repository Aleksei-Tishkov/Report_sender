import json
import os

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


