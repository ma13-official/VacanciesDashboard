import datetime
import logging

from settings.config import Local, S3Paths, VacanciesPaths, LoggerSettings
from logic.s3 import S3


class Logger:
    check_all = VacanciesPaths.start_check_all
    number_of_days = VacanciesPaths.start_number_of_days_checking

    detailed = LoggerSettings.detailed
    default = LoggerSettings.default

    detailed_check_all_logger = logging.getLogger('dc')
    check_all_logger = logging.getLogger('dfc')

    timestamp = datetime.datetime.today().strftime('%Y-%m-%dT%H--%M--%S')
    name1 = f"detailed_group/{timestamp}.log"
    name2 = f"group/{timestamp}.log"

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

        if Logger.default:

            if Logger.check_all:
                Logger.check_all_logger.setLevel(logging.INFO)
                fh2 = logging.FileHandler(Local.logs_path + Logger.name2)
                fh2.setLevel(logging.INFO)
                fh2.setFormatter(formatter)
                Logger.check_all_logger.addHandler(fh2)
                Logger.check_all_logger.info(f"Checking of last {Logger.number_of_days} days started.")

    @staticmethod
    def info(message):
        if Logger.detailed:
            Logger.detailed_check_all_logger.info(message)

    @staticmethod
    def warning(message):
        if Logger.detailed:
            Logger.detailed_check_all_logger.warning(message)
        if Logger.default:
            Logger.check_all_logger.warning(message)

    @staticmethod
    def error(message):
        if Logger.detailed:
            Logger.error_check_all(message)
        if Logger.default:
            Logger.check_all_logger.error(message, exc_info=True)

    @staticmethod
    def error_check_all(message):
        if Logger.detailed:
            Logger.detailed_check_all_logger.error(message, exc_info=True)
        if Logger.default:
            Logger.check_all_logger.error(message, exc_info=True)

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
                try:
                    S3.upload_without_remove(Local.logs_path + Logger.name1, S3Paths.logs_path + Logger.name1)
                except Exception as e:
                    print(e)

        if Logger.default:
            if Logger.check_all:
                try:
                    S3.upload_without_remove(Local.logs_path + Logger.name2, S3Paths.logs_path + Logger.name2)
                except Exception as e:
                    print(e)


class EasyLogger:
    @staticmethod
    def create(name):
        logging.basicConfig(level=logging.INFO, filename=name, filemode="w",
                            format="%(asctime)s %(levelname)s %(message)s")

    @staticmethod
    def info(message):
        logging.info(message)
