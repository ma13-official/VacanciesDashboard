import sys
import datetime, json

sys.path.append('/home/collector/VacanciesDashboard/src')

from time import perf_counter as pc
from logic.vacancies import Vacancies
from logic.logger import Logger
from logic.jsons import JSONs
from logic.id_storage import IdStorage
from logic.json_sql_loader import JSONSQLDownloader
from logic.parse_the_desc import Connecter
from logic.update_exchange_rates import update_exchange_rates

def start():
    """
    :return: запуск приложения
    """
    number_of_days = 30
    start = pc()
    Logger.create()

    Vacancies().check_all(number_of_days, get_30_days = True, now=datetime.date.today())

    vacancies_dict = IdStorage.get_30_days_dict()
    vacancies_ids = list(vacancies_dict.values())[0]
    date = list(vacancies_dict.keys())[0]

    ids = JSONs.upload_from_arr(vacancies_ids, date)
    JSONSQLDownloader.update_active(vacancies_ids)
    Connecter.parse_skills(skills=ids)
    update_exchange_rates()
    
    Logger.save()
    Logger.warning(f"Program works {round(pc() - start)//60} minutes and {round(pc() - start)%60} seconds")

    # with open('/home/collector/VacanciesDashboard/skills.json', 'r') as file:
    #     replacements = json.load(file)

    # JSONSQLDownloader.connect_to_db()
    # cur = JSONSQLDownloader.connection.cursor()
    
    # for old_word, new_word in replacements.items():
    # cur.execute(f"UPDATE skills_clear_test SET skill = REPLACE(skill, %s, %s)", (old_word, new_word))
    # cur.execute('SELECT * FROM skills_clear_test')
    # print(cur.fetchall())  

start()
