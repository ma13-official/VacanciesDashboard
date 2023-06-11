import json, os

with open('/home/collector/VacanciesDashboard/src/settings/config.json') as config:
    config = json.load(config)


class Local:
    local = config['local']

    path = local['path']
    logs_path = path + local['logs_path']
    single_jsons_path = path + local['single_jsons_path']
    group_jsons_path = path + local['group_jsons_path']
    vacancies_json_path = path + local['vacancies_json']
    errors_path = path + local['errors_path']
    captcha = errors_path + local['captcha']
    unexpected_error = errors_path + local['unexpected_error']
    captcha_images = path + local['captcha_images']
    captcha_txt = path + local['captcha_txt']


class S3Paths:
    s3 = config['s3']

    path = s3['path']
    logs_path = path + s3['logs_path']
    vacancies_jsons_path = path + s3['vacancies_jsons_path']
    group_jsons_path = path + s3['group_jsons_path']
    bucket = s3['bucket']
    profile = s3['profile']
    vacancies_json_path = path + s3['vacancies_json']


class VacanciesPaths:
    start = config['vacancies']['start']

    start_check_all = start['check_all']
    start_upload_jsons = start['upload_jsons']
    start_number_of_days_checking = start['number_of_days_checking']


class LoggerSettings:
    logger = config['logger']

    detailed = logger['detailed']
    default = logger['default']


for path in [Local.logs_path, Local.single_jsons_path, Local.group_jsons_path, Local.errors_path, Local.captcha,
             Local.unexpected_error, Local.captcha_images, LoggerSettings.default]:
    if not os.path.exists(path):
        os.makedirs(path)

if not os.path.exists(Local.vacancies_json_path):
    with open(Local.vacancies_json_path, 'w') as f:
        f.write('{}')
