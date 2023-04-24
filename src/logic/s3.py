import os
import boto3

from src.settings.config import S3Paths


class S3:
    bucket = S3Paths.bucket
    profile = S3Paths.profile
    vacancies_json_path = S3Paths.vacancies_json_path
    session = boto3.session.Session()
    s3 = session.client(service_name='s3', endpoint_url='https://storage.yandexcloud.net')

    @staticmethod
    def upload(host_path, s3_path):
        S3.s3.upload_file(host_path, S3.bucket, s3_path)
        os.remove(host_path)

    @staticmethod
    def upload_without_remove(host_path, s3_path):
        S3.s3.upload_file(host_path, S3.bucket, s3_path)

    @staticmethod
    def download(s3_path):
        get_object_response = S3.s3.get_object(Bucket=S3.bucket, Key=s3_path)
        file = get_object_response['Body'].read().decode('utf-8').replace("\\", "")[1:-1]
        return eval(file)
