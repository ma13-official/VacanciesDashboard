import threading
import datetime

from logic.api_hh_connect import connect
from logic.jsons import JSONs
from logic.logger import Logger
from logic.id_storage import IdStorage


class Vacancies:
    connect_counter = 0

    """
    Класс Vacancies представляет собой работу с API HH.ru.
    Создан для сбора информации о вакансиях на сайте HH.ru и последующего её анализа.
    Основной проблемой при сборе информации о вакансиях являются параметры пагинации API_HH,
    позволяющие за один запрос собирать информацию лишь о 100 вакансиях.
    При этом, меняя параметр page от 1 до 20, JSON возвращается с вакансиями лежащими на следующих страницах.
    Таким образом, не меняя параметры запроса (за исключением параметра page) возможно посмотреть лишь 2000 вакансий.

    В связи с этим, если значение поля 'found' JSON файла превышает 2000 - необходимо изменять параметры запроса.
    В ином случае запускается сбор вакансий со всех 20 страниц.

    На данный момент запросы делаются только по ИТ-специализации.
    Программа получает количество дней, за которые необходимо собрать все появившиеся вакансии.
    Практически всегда вакансий более 2000, следовательно, необходимо изменять параметры запросов.
    После запроса за несколько дней, программа делает запросы собирающие вакансии, по каждому дню отдельно.
    Если же за день вакансий снова более 2000, программа аналогично собирает вакансию по каждому часу внутри дня.
    Еще ни разу программа не сталкивалась с более чем 2000 вакансиями, за час.
    Если такая ситуация встретится, необходимо будет собирать вакансии по десяткам минут внутри часа(не реализовано).

    Из каждого поля вакансии в групповом JSON есть возможность собрать много базовой информации,
    которая сохраняется в датасеты. Однако не всю информацию о вакансии можно собрать таким образом.
    Поэтому помимо сохранения датасетов и групповых JSON-файлов,
    отдельно сохраняются датасеты только с id и ссылкой на отдельный JSON вакансии.

    Отдельные JSON загружаются долго по причине долгих ответов со стороны сервера.
    (один запрос - 0.2 секунды, 10000 вакансий - 2000 секунд - более 30 минут)
    Планируется реализация многопоточности для того, чтобы отправлять несколько запросов одновременно.
    """

    def check_all(self, days, now = datetime.date.today()):
        """
        Метод делает запрос с параметрами даты от "$days дней назад" до сегодняшнего числа.
        Если вакансий за этот период больше 2000, то программа отправляется в метод separating_by_days.
        Иначе - в метод check_pages.
        :param days: количество дней, за которые необходимо проверить вакансии.
        :return: продолжение программы
        """
        query = "vacancies"
        today = datetime.datetime(now.year, now.month, now.day, 0, 0)  # сегодня в 00:00:00
        date_from = (today - datetime.timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S")
        date_to = today.strftime("%Y-%m-%dT%H:%M:%S")
        params = {
            'professional_role': ['156', '160', '10', '12', '150', '25', '165', '34', '36', '73', '155', '96', '164',
                                  '104', '157', '107', '112', '113', '148', '114', '116', '121', '124', '125', '126'],
            'date_from': date_from, 'date_to': date_to, 'per_page': 100}
        vacancies = connect(query, params)
        # self.connect_counter += 1
        # Logger.info_check_all(self.connect_counter)
        founded = vacancies['found']
        Logger.warning_check_all(f"Founded {founded} vacancies")
        if founded > 2000:
            self.separating_by_days(query, params, today, days)
        else:
            self.check_pages(query, params)
            Logger.warning_check_all(
                f"From {params['date_from']} to {params['date_to']} founded less than 2000 vacancies!")

        date = (today - datetime.timedelta(days=days)).strftime("%Y-%m-%d")
        IdStorage.update_vacancies_dict(date)
        IdStorage.update_vacancies_dict(date)

    def separating_by_days(self, query, params, today, days):
        """
        Выполняет запросы собирающие вакансии, по каждому дню отдельно.
        Если вакансий больше 2000 - separating_by_hours.
        :param query: запрос
        :param params: параметры запроса
        :param today: сегодня в 00:00:00
        :param days: количество дней, за которые надо проверить каждый день отдельно
        :return: продолжение программы
        """
        for days_ago in range(days):
            params['date_to'] = (today - datetime.timedelta(days=days_ago)).strftime("%Y-%m-%dT%H:%M:%S")
            params['date_from'] = (today - datetime.timedelta(days=days_ago + 1)).strftime("%Y-%m-%dT%H:%M:%S")
            params['page'] = None
            vacancies = connect(query, params)
            # self.connect_counter += 1
            # Logger.info_check_all(self.connect_counter)
            founded = vacancies['found']
            Logger.warning_check_all(f"{params['date_from'][:10]} founded {founded} vacancies")
            cur_day = today - datetime.timedelta(days=days_ago)
            if founded > 2000:
                self.separating_by_hours(query, cur_day)
            else:
                self.check_pages(query, params)

    def separating_by_hours(self, query, cur_day):
        """
        Выполняет запросы собирающие вакансии, по каждому часу отдельно внутри заданного дня.
        Запросы по часам выполняются многопоточно, для увеличения скорости выполнения программы.
        Если вакансий больше 2000 - ошибка.
        :param query: запрос
        :param cur_day: день в котором необходимо проверить каждый час.
        :return: продолжение программы.
        """
        for hour in range(0, 24):
            params = {'professional_role': ['156', '160', '10', '12', '150', '25', '165', '34', '36', '73', '155', '96',
                                            '164', '104', '157', '107', '112', '113', '148', '114', '116', '121', '124',
                                            '125', '126'],
                      'per_page': 100,
                      'date_from': (cur_day - datetime.timedelta(hours=hour + 1)).strftime("%Y-%m-%dT%H:%M:%S"),
                      'date_to': (cur_day - datetime.timedelta(hours=hour)).strftime("%Y-%m-%dT%H:%M:%S")}
            self.query_by_hours(query, params)

    def threads(self, query, cur_day, hour):
        """
        Запуск многопоточных запросов внутри заданного дня.
        :param query: запрос.
        :param cur_day: день в котором необходимо проверить каждый час.
        :param hour: час, в котором необходимо собрать вакансии.
        :return: продолжение программы
        """
        threads = []
        for x in range(4):
            params = {'professional_role': ['156', '160', '10', '12', '150', '25', '165', '34', '36', '73', '155', '96',
                                            '164', '104', '157', '107', '112', '113', '148', '114', '116', '121', '124',
                                            '125', '126'],
                      'per_page': 100,
                      'date_from': (cur_day - datetime.timedelta(hours=hour + x + 1)).strftime("%Y-%m-%dT%H:%M:%S"),
                      'date_to': (cur_day - datetime.timedelta(hours=hour + x)).strftime("%Y-%m-%dT%H:%M:%S")}
            t = threading.Thread(target=self.query_by_hours, args=(query, params))
            threads.append(t)
            t.start()
            params = {}
        for t in threads:
            t.join()

    def query_by_hours(self, query, params):
        """
        Выполнение запросов
        :param query: запрос
        :param params: параметры
        :return: продолжение программы
        """
        vacancies = connect(query, params)
        # self.connect_counter += 1
        # Logger.info_check_all(self.connect_counter)
        if vacancies['found'] > 2000:
            # Logger.warning_check_all(f"From {params['date_from']} to {params['date_to']} founded more than 2000 vacancies!")
            self.query_by_minutes(query, params)
        else:
            self.check_pages(query, params)

    def query_by_minutes(self, query, params):
        prev_date_to = params['date_to']
        prev_date_from = params['date_from']
        for i in range(0, 6):
            params['date_from'] = prev_date_from[:-5] + str(i) + prev_date_from[-4:]
            if i == 5:
                params['date_to'] = prev_date_to
            else:
                params['date_to'] = prev_date_from[:-5] + str(i + 1) + prev_date_from[-4:]
            vacancies = connect(query, params)
            # self.connect_counter += 1
            # Logger.info_check_all(self.connect_counter)
            if vacancies['found'] > 2000:
                date_to_save = params['date_to']
                params['date_to'] = prev_date_from[:-5] + str(i) + '5' + prev_date_from[-3:]
                self.check_pages(query, params)
                params['date_from'] = params['date_to']
                params['date_to'] = date_to_save
                self.check_pages(query, params)
            else:
                self.check_pages(query, params)

    def check_pages(self, query, params):
        """
        Выполнение запросов меняя параметр page
        :param query: запрос
        :param params: параметры
        :return: продолжение программы
        """
        params['page'] = None
        vacancies = connect(query, params)
        # self.connect_counter += 1
        # Logger.info_check_all(self.connect_counter)

        Logger.info_check_all(
            f"From {params['date_from']} to {params['date_to']} founded {vacancies['found']} vacancies")
        try:
            pages = vacancies['found'] // 100
        except:
            print(vacancies)
            input()
        if pages > 0:
            for i in range(pages + 1):
                params['page'] = i
                vacancies = connect(query, params)
                # self.connect_counter += 1
                # Logger.info_check_all(self.connect_counter)
                JSONs.save_group_vacancies_json(vacancies, params)
        else:
            JSONs.save_group_vacancies_json(vacancies, params)
