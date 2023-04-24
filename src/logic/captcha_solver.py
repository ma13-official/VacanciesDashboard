import os
import urllib.request

from selenium import webdriver
from twocaptcha import TwoCaptcha
from logic.logger import Logger
from settings.config import Local

def solve(url):
    # инициализация веб-драйвера
    driver = webdriver.Chrome()

    # переход на страницу
    driver.get(url)

    # находим URL изображения на странице
    image_url = driver.find_element("xpath", "//img").get_attribute("src")

    solver = TwoCaptcha('ffe223bb62e346cf3bfe7ae636e3837e')

    code = image_solver(image_url, solver)

    # находим поле для ввода текста и вводим текст
    text_input = driver.find_element("name", "captchaText")
    text_input.send_keys(code)

    # нажимаем на кнопку отправки
    submit_button = driver.find_element("xpath", "//button[@data-qa='account-captcha-submit']")
    submit_button.click()

    # закрытие веб-драйвера
    driver.close()

def image_solver(image_url, solver):
    # скачиваем изображение по URL
    urllib.request.urlretrieve(image_url, Local.captcha_images + "1234.jpg")
    result = None
    try:
        result = solver.normal(Local.captcha_images + "1234.jpg", lang='ru')
    except Exception as e:
        Logger.error(e)
        if e == 'ERROR_IMAGE_TYPE_NOT_SUPPORTED':
            return ''
        image_solver(image_url, solver)
    os.rename(Local.captcha_images + "1234.jpg", Local.captcha_images + f'{result["captchaId"]}.jpg')
    Logger.info(f'Captcha solved!   {result}')
    return result['code']
