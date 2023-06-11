import sys

sys.path.append('/home/collector/VacanciesDashboard/src')

from time import perf_counter as pc
from logic.jsons import JSONs
from logic.logger import Logger


def start():
    """
    :return: запуск приложения
    """
    start = pc()
    number_of_days = 1
    Logger.create_for_upload()
    JSONs.json_upload(number_of_days)
    Logger.save()
    Logger.info(f"Program works {round(pc() - start)} seconds")


start()
