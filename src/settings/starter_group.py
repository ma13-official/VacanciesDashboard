import sys
import datetime

sys.path.append('/home/collector/VacanciesDashboard/src')

from time import perf_counter as pc
from logic.vacancies import Vacancies
from logic.logger import Logger
from logic.jsons import JSONs
from logic.id_storage import IdStorage
from logic.parse_the_desc import Connecter

def start():
    """
    :return: запуск приложения
    """
    number_of_days = 1
    today = datetime.date.today()
    Logger.create()
    st = pc()
    try:
        Vacancies().check_all(number_of_days, now=today)
        ids = JSONs.json_upload(number_of_days, today=today)
        Connecter.parse_skills(skills = ids)
    except Exception as e:
        Logger.error(e)
    Logger.save()
    Logger.warning(f"Program works {round(pc() - st)//60} minutes and {round(pc() - st)%60} seconds")


start()
