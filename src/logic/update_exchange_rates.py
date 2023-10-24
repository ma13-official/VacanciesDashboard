import requests

from logic.json_sql_loader import JSONSQLDownloader


def update_exchange_rates():
    # JSONSQLDownloader.connect_to_db()
    cursor = JSONSQLDownloader.connection.cursor()

    currencies = {}
    dictionaries = requests.get('https://api.hh.ru/dictionaries').json()
    # currencies['NULL'] = 1.00
    for currency in dictionaries['currency']:
        query = f"UPDATE exchange_rates SET rate = {round(1/currency['rate'], 2)} WHERE currency = '{currency['code']}'"
        cursor.execute(query)
        JSONSQLDownloader.connection.commit()

    query = f"UPDATE exchange_rates SET rate = 1.00 WHERE currency is NULL"
    cursor.execute(query)
    JSONSQLDownloader.connection.commit()

