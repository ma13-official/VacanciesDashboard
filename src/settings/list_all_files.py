import json
import boto3
from datetime import datetime, timedelta
# from logic.vacancies import Vacancies

bucket_name = 'data-brains-bucket'
profile = 'default'
local = '/home/collector/hh_json_downloader/json_urls/'
session = boto3.session.Session()
s3 = session.client(service_name='s3', endpoint_url='https://storage.yandexcloud.net')
out_dict = dict()


def get_dates_list(start_date, end_date=datetime.now().date()-timedelta(days=1)):
    dates_list = []
    current_date = start_date
    while current_date <= end_date:
        dates_list.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
    return dates_list

start_date = datetime.strptime("2022-12-01", "%Y-%m-%d").date()
end_date = datetime.strptime("2023-06-08", "%Y-%m-%d").date()
dates = get_dates_list(start_date, end_date=end_date)

update_dates = []

for date in dates:
    # print(date)
    results = []
    prefix = f'hh.ru/vacancies_jsons/{date}/'

    if s3.list_objects(Bucket=bucket_name, Prefix=prefix).get('Contents') is None:
        print(date)
        update_dates.append(date)

    def get_all_objects(bucket_name, prefix):
        continuation_token = None
        while True:
            if continuation_token:
                objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, ContinuationToken=continuation_token)
            else:
                objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

            contents = objects.get("Contents")
            for obj in contents:
                date = prefix.split('/')[2]  # Извлекаем часть строки, содержащую дату
                year = date[:4]
                month = date[5:7]
                day = date[8:]
                Vacancies().check_all(1, year, month, day)
                name = obj.get("Key").split('/')[-1][:-5]
                results.append(name)

            continuation_token = objects.get("NextContinuationToken")
            if not continuation_token:
                break

    # get_all_objects(bucket_name, prefix)

    # out_dict[date] = results

# with open("s3_vacancies_jsons.json", "w") as file:
#     json.dump(dict(sorted(out_dict.items())), file, indent=4)
    
with open("update_date,json", "w") as file:
    json.dump(update_dates, file, indent=4)