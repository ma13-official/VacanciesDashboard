import json
import os

from datetime import datetime, timedelta, date
from time import perf_counter as pc
from logic.api_hh_connect import Connector
from logic.s3 import S3
from settings.config import Local, S3Paths
from logic.logger import Logger
from logic.json_sql_loader import JSONSQLDownloader


class JSONs:
    vacancies_json = []
    vacancies_json_host_path = Local.vacancies_json_path
    vacancies_json_s3_path = S3Paths.vacancies_json_path

    @staticmethod
    def save_group_vacancies_json(vacancies, params, get_30_days=False):
        """
        Сохранение группового JSON-файла и выгрузка его в S3.
        :param vacancies: групповой JSON-файл
        :param params: параметры запроса.
        :return: выгруженные файлы.
        """
        page = "" if params['page'] is None else f"---{params['page']}"
        if get_30_days:
            host_path = f"{Local.group_jsons_path}{date.today()}/{params['date_from'][:13]}{page}.json"
            s3_path = f"{S3Paths.not_archive_path}{date.today()}/{params['date_from'][:10]}/{params['date_from'][:16]}{page}.json"
        else:
            host_path = f"{Local.group_jsons_path}{params['date_from'][:13]}{page}.json"
            s3_path = f"{S3Paths.group_jsons_path}{params['date_from'][:10]}/{params['date_from'][:16]}{page}.json"
        if not os.path.exists(f"{Local.group_jsons_path}{date.today()}/"):
            os.makedirs(f"{Local.group_jsons_path}{date.today()}/")
        with open(host_path, 'w', encoding='utf8') as outfile:
            json.dump(vacancies, outfile, sort_keys=False, indent=4, ensure_ascii=False, separators=(',', ': '))
        S3.upload(host_path, s3_path)

    @staticmethod
    def upload_single_jsons(url, directory, vacancy_id):
        """
        Метод созданный для многопоточной выгрузки одиночных JSON
        :param url: url JSON
        :param directory: имя папки
        :param vacancy_id: имя файла
        :return: выгруженные файлы.
        """
        host_path = f'{Local.group_jsons_path}{directory}/{vacancy_id}.json'
        s3_path = f'{S3Paths.vacancies_jsons_path}{directory}/{vacancy_id}.json'
        if not os.path.exists(host_path):
            try:
                vacancy = JSONs.C.connect(url)
            except Exception as e:
                print(e)
                input()
                return
            with open(host_path, 'w', encoding='utf8') as outfile:
                json.dump(vacancy, outfile, sort_keys=False, indent=4, ensure_ascii=False, separators=(',', ': '))
            JSONSQLDownloader.upload_file_to_db(host_path)
        S3.upload(host_path, s3_path)

    @staticmethod
    def json_upload(number_of_days, today=datetime.today()):
        with open(Local.vacancies_json_path, 'r') as f:
            vacancies_dict = json.load(f)

        JSONSQLDownloader.connect_to_db()
        JSONs.C = Connector()

        start = pc()
        founded, not_founded, total = 0, 0, 0
        values = []

        for i in range(number_of_days):
            key = (today - timedelta(days=1 + i)).strftime('%Y-%m-%d')
            arr = vacancies_dict[key]
            JSONs.make_dir(f'{Local.group_jsons_path}', key)
            Logger.warning(f"{key} started uploading, {len(arr)} JSONs founded")
            cur_not_founded = 0
            for value in arr:
                total += 1
                url = f'https://api.hh.ru/vacancies/{value}?host=hh.ru '

                if JSONSQLDownloader.check_existence('vacancy', value):
                    founded += 1
                else:
                    not_founded += 1
                    cur_not_founded += 1
                    if not_founded % 100 == 0:
                        Logger.warning(f'{value} {founded+not_founded} {not_founded} {founded} {pc() - start}')
                    else:
                        Logger.info(f'{value} {founded+not_founded} {not_founded} {founded} {pc() - start}')
                    JSONs.upload_single_jsons(url, key, value)
                    values.append(value)
                    
                print(total, founded, not_founded, end='\r', flush=True)
            Logger.warning(f"{key} uploaded, {cur_not_founded} JSONs downloaded")

        JSONSQLDownloader.update_mv()
        
        return values

    @staticmethod
    def upload_from_arr(arr, key):
        JSONSQLDownloader.connect_to_db()
        JSONs.C = Connector()

        start = pc()
        founded, not_founded, total = 0, 0, 0
        values = []

        Logger.warning(f"Upload started, {len(arr)} JSONs founded")
        cur_not_founded = 0
        for value in arr:

            total += 1
            url = f'https://api.hh.ru/vacancies/{value}?host=hh.ru '

            if JSONSQLDownloader.check_existence('vacancy', value):
                founded += 1
            else:
                not_founded += 1
                cur_not_founded += 1
                if not_founded % 100 == 0:
                    Logger.warning(f'{value} {founded+not_founded} {not_founded} {founded} {pc() - start}')
                else:
                    Logger.info(f'{value} {founded+not_founded} {not_founded} {founded} {pc() - start}')
                JSONs.upload_single_jsons(url, key, value)
                values.append(value)
                
        Logger.warning(f'{value} {founded+not_founded} {not_founded} {founded} {pc() - start}')

        return values


    @staticmethod
    def make_dir(path, directory):
        os.chdir(path)
        if not os.path.exists(os.path.join(path, directory)):
            os.mkdir(directory)
