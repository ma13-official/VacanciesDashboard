import json
from logic.s3 import S3
from datetime import date as date_c
from logic.logger import Logger
from settings.config import Local, S3Paths


class IdStorage:
    @staticmethod
    def get_30_days_dict():
        return IdStorage.static_adding(date_c.today().strftime("%Y-%m-%d"), dict(), True)

    @staticmethod
    def static_making_dict_of_ids(date):
        vacancies_dict = json.load(open(Local.vacancies_json_path))
        vacancies_dict = IdStorage.static_adding(date, vacancies_dict)
        with open(Local.vacancies_json_path, "w") as file:
            json.dump(dict(sorted(vacancies_dict.items())), file, indent=4)

        vacancies = sum([len(sub) for sub in vacancies_dict.values()])
        Logger.info(f"{vacancies} vacancies in vacancies_dict.json now.")

    @staticmethod
    def static_adding(dir, vacancies_dict, get_30_days=False):
        if get_30_days:
            prefix = f"{S3Paths.not_archive_path}{dir}"
        else:
            prefix = f'{S3Paths.group_jsons_path}{dir}'
        data = []
        continuation_token = None
        while True:
            if continuation_token:
                objects = S3.s3.list_objects_v2(Bucket=S3.bucket, Prefix=prefix, ContinuationToken=continuation_token)
            else:
                objects = S3.s3.list_objects_v2(Bucket=S3.bucket, Prefix=prefix)

            for obj in objects.get("Contents"):
                keyString = obj.get("Key")
                get_object_response = S3.s3.get_object(Bucket=S3.bucket, Key=keyString)
                file_content = get_object_response['Body'].read().decode('utf-8')
                json_data = json.loads(file_content)
                try:
                    for item in json_data['items']:
                        data.append(item['id'])
                except:
                    Logger.info(str(json_data))

            continuation_token = objects.get("NextContinuationToken")
            if not continuation_token:
                break

        vacancies_dict[dir] = data    
        return vacancies_dict

    @classmethod
    def making_dict_of_ids(cls, date):
        cls.vacancies_dict = json.load(open(Local.vacancies_json_path))
        cls.adding(date)
        IdStorage.date = date
        with open(Local.vacancies_json_path, "w") as file:
            json.dump(dict(sorted(cls.vacancies_dict.items())), file, indent=4)

        vacancies = sum([len(sub) for sub in cls.vacancies_dict.values()])
        Logger.info(f"{vacancies} vacancies in vacancies_dict.json now.")

    @classmethod
    def adding(cls, dir):
        prefix = f'hh.ru/group_jsons/' + dir
        data = []
        continuation_token = None
        while True:
            if continuation_token:
                objects = S3.s3.list_objects_v2(Bucket=S3.bucket, Prefix=prefix, ContinuationToken=continuation_token)
            else:
                objects = S3.s3.list_objects_v2(Bucket=S3.bucket, Prefix=prefix)

            for obj in objects.get("Contents"):
                keyString = obj.get("Key")
                get_object_response = S3.s3.get_object(Bucket=S3.bucket, Key=keyString)
                file_content = get_object_response['Body'].read().decode('utf-8')
                json_data = json.loads(file_content)
                try:
                    for item in json_data['items']:
                        data.append(item['id'])
                except:
                    Logger.info(str(json_data))

            continuation_token = objects.get("NextContinuationToken")
            if not continuation_token:
                break

        cls.vacancies_dict[dir] = data
