import os
import urllib.request

from selenium import webdriver
from twocaptcha import TwoCaptcha
from logic.logger import Logger
from settings.config import Local
from dotenv import dotenv_values


def solve(url):
    # инициализация веб-драйвера
    driver = webdriver.Chrome()

    # переход на страницу
    driver.get(url)

    # находим URL изображения на странице
    image_url = driver.find_element("xpath", "//img").get_attribute("src")

    if image_url == '':
        driver.close()
        return

    token = dotenv_values(Local.env)['2C']

    solver = TwoCaptcha(token)

    code = image_solver(image_url, solver)

    # находим поле для ввода текста и вводим текст
    text_input = driver.find_element("name", "captchaText")
    text_input.send_keys(code)

    # previous_title = driver.title

    # нажимаем на кнопку отправки
    submit_button = driver.find_element("xpath", "//button[@data-qa='account-captcha-submit']")
    submit_button.click()

    # current_title = driver.title

    # if current_title != previous_title:
    #     print("Страница изменилась.")
    # else:
    #     print("Страница не изменилась.")
    #     solve(url)


    # # print(driver.find_element("xpath", "//body[contains(@class, 'vsc-initialized')]").text)
    # html = driver.page_source

    # # Указываете путь и имя файла, в который будет сохранен HTML-код
    # file_path = "/home/collector/VacanciesDashboard/file.html"

    # # Открываем файл в режиме записи
    # with open(file_path, "w", encoding="utf-8") as file:
    #     # Записываем HTML-код в файл
    #     file.write(html)

    # print("HTML-код сохранен в файл:", file_path)

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
    Logger.warning(f'Captcha solved!   {result}')
    return result['code']

# solve('https://hh.ru/account/captcha?state=pxvcxBozfu7ry7R4QCetFpRgByk-d8c77C_TwpAbKNKURyNV7jIN3ZmLkwVMlGAXdz3G5LzKcxNFkzJ1yL6uxsjvCWIqJ4meQmq9GgigDD8HWmURsgy21IrmPqWIP92i&backurl=vacancies')


# def solve_url(url):

#     token = dotenv_values('/home/collector/VacanciesDashboard/src/logic/.env')['2C']

#     solver = TwoCaptcha(token)

#     code = image_solver(image_url, solver)