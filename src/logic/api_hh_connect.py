import json
import requests
import time

from datetime import datetime
from logic.logger import Logger
from settings.config import Local
from logic.captcha_solver import solve


def connect(query, params=None):
    if params is None:
        params = {}
    if not query.startswith("https://api.hh.ru/"):
        url = "https://api.hh.ru/" + query
    else:
        url = query
    data = requests.get(url, params).json()
    if list(data.keys())[0] == 'errors':
        match data['errors'][0]['type']:
            case 'captcha_required':
                captcha_url = data['errors'][0]['captcha_url'] + '&backurl=' + query

                with open(Local.captcha_txt, mode="a") as myfile:
                    myfile.write(f"\n\n{datetime.now()}   {captcha_url}")

                Logger.warning(f"Captcha: {captcha_url}")

                with open(
                        f"{Local.captcha}{data['request_id']}.json",
                        'w', encoding='utf8') as outfile:
                    json.dump(data, outfile, sort_keys=False, indent=4, ensure_ascii=False,
                              separators=(',', ': '))

                solve(captcha_url)
                time.sleep(5)

                data = connect(query, params)
            case _:
                Logger.critical_check_all("Unexpected error!!!")
                with open(
                        f"{Local.unexpected_error}{data['request_id']}.json",
                        'w', encoding='utf8') as outfile:
                    json.dump(data, outfile, sort_keys=False, indent=4, ensure_ascii=False,
                              separators=(',', ': '))
                input()
    else:
        params['backurl'] = None
    while 'found' not in data.keys():
            data = connect(query, params)
    return data
