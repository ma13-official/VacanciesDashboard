import sys

sys.path.append('/home/collector/VacanciesDashboard/src')

from time import perf_counter as pc
from src.logic.vacancies import Vacancies
from src.logic.logger import Logger


def start():
    """
    :return: запуск приложения
    """
    number_of_days = 3
    start = pc()
    Logger.create()
    Vacancies().check_all(number_of_days)
    Logger.save()
    Logger.info(f"Program works {round(pc() - start)} seconds")


start()
