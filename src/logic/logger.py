import datetime
import logging

from settings.config import Local, S3Paths, VacanciesPaths, LoggerSettings
from logic.s3 import S3


class Logger:
    check_all = VacanciesPaths.start_check_all
    number_of_days = VacanciesPaths.start_number_of_days_checking
    upload_jsons = VacanciesPaths.start_upload_jsons

    detailed = LoggerSettings.detailed
    default = LoggerSettings.default

    detailed_check_all_logger = logging.getLogger('dc')
    detailed_upload_logger = logging.getLogger('du')
    check_all_logger = logging.getLogger('dfc')
    upload_logger = logging.getLogger('dfu')

    name1 = f"detailed_group/{datetime.datetime.today().strftime('%Y-%m-%dT%H--%M--%S')}.log"
    name2 = f"detailed_single/{datetime.datetime.today().strftime('%Y-%m-%dT%H--%M--%S')}.log"
    name3 = f"group/{datetime.datetime.today().strftime('%Y-%m-%dT%H--%M--%S')}.log"
    name4 = f"single/{datetime.datetime.today().strftime('%Y-%m-%dT%H--%M--%S')}.log"

    @staticmethod
    def create():
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

        if Logger.detailed:

            if Logger.check_all:
                Logger.detailed_check_all_logger.setLevel(logging.INFO)
                fh1 = logging.FileHandler(Local.logs_path + Logger.name1)
                fh1.setLevel(logging.INFO)
                fh1.setFormatter(formatter)
                Logger.detailed_check_all_logger.addHandler(fh1)
                Logger.detailed_check_all_logger.info(f"Checking of last {Logger.number_of_days} days started.")

            if Logger.upload_jsons:
                Logger.detailed_upload_logger.setLevel(logging.INFO)
                fh2 = logging.FileHandler(Local.logs_path + Logger.name2)
                fh2.setLevel(logging.INFO)
                fh2.setFormatter(formatter)
                Logger.detailed_upload_logger.addHandler(fh2)
                Logger.detailed_upload_logger.info("Upload of single vacancies JSONs started.")

        if Logger.default:

            if Logger.check_all:
                Logger.check_all_logger.setLevel(logging.INFO)
                fh3 = logging.FileHandler(Local.logs_path + Logger.name3)
                fh3.setLevel(logging.INFO)
                fh3.setFormatter(formatter)
                Logger.check_all_logger.addHandler(fh3)
                Logger.check_all_logger.info(f"Checking of last {Logger.number_of_days} days started.")

            if Logger.upload_jsons:
                Logger.upload_logger.setLevel(logging.INFO)
                fh4 = logging.FileHandler(Local.logs_path + Logger.name4)
                fh4.setLevel(logging.INFO)
                fh4.setFormatter(formatter)
                Logger.upload_logger.addHandler(fh4)
                Logger.upload_logger.info("Upload of single vacancies JSONs started.")

    @staticmethod
    def create_for_upload():
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

        Logger.detailed_upload_logger.setLevel(logging.INFO)
        fh2 = logging.FileHandler(Local.logs_path + Logger.name2)
        fh2.setLevel(logging.INFO)
        fh2.setFormatter(formatter)
        Logger.detailed_upload_logger.addHandler(fh2)
        Logger.detailed_upload_logger.info("Upload of single vacancies JSONs started.")

        Logger.upload_logger.setLevel(logging.INFO)
        fh4 = logging.FileHandler(Local.logs_path + Logger.name4)
        fh4.setLevel(logging.INFO)
        fh4.setFormatter(formatter)
        Logger.upload_logger.addHandler(fh4)
        Logger.upload_logger.info("Upload of single vacancies JSONs started.")

    @staticmethod
    def info(message):
        if Logger.detailed:
            Logger.detailed_check_all_logger.info(message)
            Logger.detailed_upload_logger.info(message)
        # if Logger.default:
        #     Logger.check_all_logger.info(message)
        #     Logger.upload_logger.info(message)

    @staticmethod
    def info_check_all(message):
        if Logger.detailed:
            Logger.detailed_check_all_logger.info(message)
        # if Logger.default:
        #     Logger.check_all_logger.info(message)

    @staticmethod
    def info_upload(message):
        if Logger.detailed:
            Logger.detailed_upload_logger.info(message)
        # if Logger.default:
        #     Logger.upload_logger.info(message)

    @staticmethod
    def warning(message):
        if Logger.detailed:
            Logger.warning_check_all(message)
            Logger.warning_upload(message)
        if Logger.default:
            Logger.check_all_logger.warning(message)
            Logger.upload_logger.warning(message)

    @staticmethod
    def warning_check_all(message):
        if Logger.detailed:
            Logger.detailed_check_all_logger.warning(message)
        if Logger.default:
            Logger.check_all_logger.warning(message)

    @staticmethod
    def warning_upload(message):
        if Logger.detailed:
            Logger.detailed_upload_logger.warning(message)
        if Logger.default:
            Logger.upload_logger.warning(message)

    @staticmethod
    def error(message):
        if Logger.detailed:
            Logger.error_check_all(message)
            Logger.error_upload(message)
        if Logger.default:
            Logger.check_all_logger.error(message, exc_info=True)
            Logger.upload_logger.error(message, exc_info=True)

    @staticmethod
    def error_check_all(message):
        if Logger.detailed:
            Logger.detailed_check_all_logger.error(message, exc_info=True)
        if Logger.default:
            Logger.check_all_logger.error(message, exc_info=True)

    @staticmethod
    def error_upload(message):
        if Logger.detailed:
            Logger.detailed_upload_logger.error(message, exc_info=True)
        if Logger.default:
            Logger.upload_logger.error(message, exc_info=True)

    @staticmethod
    def critical_check_all(message):
        if Logger.detailed:
            Logger.detailed_check_all_logger.critical(message)
        if Logger.default:
            Logger.check_all_logger.critical(message)

    @staticmethod
    def save():
        if Logger.detailed:
            if Logger.check_all:
                S3.upload_without_remove(Local.logs_path + Logger.name1, S3Paths.logs_path + Logger.name1)

            if Logger.upload_jsons:
                S3.upload_without_remove(Local.logs_path + Logger.name2, S3Paths.logs_path + Logger.name2)

        if Logger.default:
            if Logger.check_all:
                S3.upload_without_remove(Local.logs_path + Logger.name3, S3Paths.logs_path + Logger.name3)

            if Logger.upload_jsons:
                S3.upload_without_remove(Local.logs_path + Logger.name4, S3Paths.logs_path + Logger.name4)


class EasyLogger:
    @staticmethod
    def create(name):
        logging.basicConfig(level=logging.INFO, filename=name, filemode="w",
                            format="%(asctime)s %(levelname)s %(message)s")

    @staticmethod
    def info(message):
        logging.info(message)
