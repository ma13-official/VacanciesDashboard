import json
from logic.s3 import S3
from datetime import datetime, timedelta
from logic.logger import Logger
from settings.config import Local, S3_paths

class IdStorage:
    vacancies_dict = json.load(open(Local.vacancies_json_path))

    @classmethod
    def making_dict_of_ids(cls, date):
        cls.adding(date)

        with open(Local.vacancies_json_path, "w") as file:
            json.dump(dict(sorted(cls.vacancies_dict.items())), file, indent=4)

        vacancies = sum([len(sub) for sub in cls.vacancies_dict.values()])
        Logger.info_check_all(f"{vacancies} vacancies in vacancies_dict.json now.")

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
                    Logger.info_check_all(str(json_data))

            continuation_token = objects.get("NextContinuationToken")
            if not continuation_token:
                break

        cls.vacancies_dict[dir] = data

    @classmethod
    def deleting_duplicates(cls):
        vacancies_dict = json.load(open(Local.vacancies_json_path))
        new_values = cls.remove_duplicates(vacancies_dict.values())
        duplicates = sum([len(sub) for sub in vacancies_dict.values()]) - sum([len(sub) for sub in new_values])
        Logger.info_check_all(f"{duplicates} duplicates founded.")
        new_dict = cls.zip_arr_and_dict(new_values, vacancies_dict)
        with open(Local.vacancies_json_path, "w") as file:
            json.dump(dict(sorted(new_dict.items())), file, indent=4)
            
    @staticmethod
    def remove_duplicates(arr):
        arr = reversed(arr)
        seen = {}
        new_arr = []
        for i, row in enumerate(arr):
            new_row = []
            for value in row:
                if value not in seen:
                    seen[value] = i
                    new_row.append(value)
                elif seen[value] == i:
                    new_row.append(value)
            new_arr.append(new_row)
        return list(reversed(new_arr))

    @staticmethod
    def zip_arr_and_dict(arr, dct):
        for key, value in zip(dct.keys(), arr):
            dct[key] = value
        return dct

    @classmethod
    def get_s3_storage(cls):
        start_date = datetime.strptime("2022-12-01", "%Y-%m-%d").date()
        dates = cls.get_dates_list(start_date)
        s3_storage = dict()

        for date in dates:
            prefix = S3_paths.vacancies_jsons_path + f'{date}/'
            s3_storage[date] = cls.get_all_objects(prefix)

        items_in_s3 = sum([len(sub) for sub in s3_storage.values()])
        Logger.info_check_all(f"{items_in_s3} items in vacancies_jsons directory now.")

        return s3_storage

    @staticmethod
    def get_dates_list(start_date):
        end_date=datetime.now().date()-timedelta(days=1)
        dates_list = []
        current_date = start_date
        while current_date <= end_date:
            dates_list.append(current_date.strftime("%Y-%m-%d"))
            current_date += timedelta(days=1)
        return dates_list
    
    @staticmethod
    def get_all_objects(prefix):
        files_in_dir = []
        continuation_token = None
        while True:
            if continuation_token:
                objects = S3.s3.list_objects_v2(Bucket=S3.bucket, Prefix=prefix, ContinuationToken=continuation_token)
            else:
                objects = S3.s3.list_objects_v2(Bucket=S3.bucket, Prefix=prefix)

            contents = objects.get("Contents")
            try:
                for obj in contents:
                    name = obj.get("Key").split('/')[-1][:-5]
                    files_in_dir.append(name)
            except:
                Logger.info_check_all(str(objects))

            continuation_token = objects.get("NextContinuationToken")
            if not continuation_token:
                break

        return files_in_dir

    @staticmethod
    def diff_in_two_dicts(file1, file2):
        results = []

        # Iterate through the values in the first JSON file
        for key1, values1 in file1.items():
            for value1 in values1:
                # Check if the value exists in the second JSON file
                found = False
                for key2, values2 in file2.items():
                    if value1 in values2:
                        # Get the key for the value in the second JSON file
                        if key1 == key2:
                            # The keys are the same, do not add to the results
                            found = True
                            break
                        else:
                            # The keys are different, add to the results
                            found = True
                            results.append((key1, key2, value1))
                            break
                if not found:
                    file2[key1].append(value1)

        return results, file2

    @classmethod
    def move_files_in_s3(cls):
        s3_storage = cls.get_s3_storage()
        moves, cls.vacancies_dict = cls.diff_in_two_dicts(s3_storage, cls.vacancies_dict)

        Logger.info_check_all(f'There are {len(moves)} differencies.')

        for move in moves:
            src_file = S3_paths.vacancies_jsons_path + move[0] + '/' + move[2] + '.json'
            dst_file = S3_paths.vacancies_jsons_path + move[1] + '/' + move[2] + '.json'

            S3.s3.copy_object(Bucket=S3.bucket, CopySource={"Bucket": S3.bucket, "Key": src_file}, Key=dst_file)
            S3.s3.delete_object(Bucket=S3.bucket, Key=src_file)

        items_in_s3 = sum([len(sub) for sub in s3_storage.values()])
        Logger.info_check_all(f"{items_in_s3} items in vacancies_jsons directory now.")
        vacancies = sum([len(sub) for sub in cls.vacancies_dict.values()])
        Logger.info_check_all(f"{vacancies} vacancies in vacancies_dict.json now.")

    @classmethod
    def update_vacancies_dict(cls, date):
        cls.making_dict_of_ids(date)
        Logger.warning_check_all("vacancies_dict.json updated.")
        cls.deleting_duplicates()
        Logger.warning_check_all("Duplicates deleted.") 
        cls.move_files_in_s3()
        Logger.warning_check_all("Files in s3 moved.")
