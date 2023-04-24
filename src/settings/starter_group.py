import sys
sys.path.append('/home/collector/VacanciesDashboard/src')

from time import perf_counter as pc
from logic.vacancies import Vacancies
from logic.logger import Logger

class Starter:
    def start():
        """
        :return: запуск приложения
        """
        number_of_days=3
        start = pc()
        Logger.create()
        Vacancies().check_all(number_of_days)
        Logger.save()
        Logger.info(f"Program works {round(pc() - start)} seconds")

Starter.start()
