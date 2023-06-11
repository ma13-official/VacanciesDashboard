import sys

sys.path.append('/home/collector/VacanciesDashboard/src')

from time import perf_counter as pc
from logic.vacancies import Vacancies
from logic.logger import Logger
from logic.jsons import JSONs


def start():
    """
    :return: запуск приложения
    """
    number_of_days = 1
    start = pc()
    Logger.create()
    Vacancies().check_all(number_of_days)
    # try:
    #     JSONs.json_upload(number_of_days)
    # except Exception as e:
    #     Logger.error(e)
    Logger.save()
    Logger.info(f"Program works {round(pc() - start)//60} minutes and {round(pc() - start)%60} seconds")


start()
