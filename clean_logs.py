import os
import shutil
from datetime import datetime, timedelta

for subfolder in ['detailed_group', 'detailed_single', 'group', 'single']:
    # Путь к папке с лог-файлами
    log_folder = '/home/collector/VacanciesDashboard/etc/logs/' + subfolder

    # Путь к папке, куда нужно перемещать старые файлы
    archive_folder = '/home/collector/VacanciesDashboard/etc/archive_logs/' + subfolder

    # Количество дней, после которых файлы считаются устаревшими
    days_threshold = 30

    # Получить текущую дату
    current_date = datetime.now()

    # Пройти по всем файлам в папке с логами
    for filename in os.listdir(log_folder):
        if filename.endswith('.log'):
            file_path = os.path.join(log_folder, filename)
            
            # Получить дату из имени файла (предполагается, что формат имени файла фиксированный)
            file_date_str = filename.split('.')[0]
            file_date = datetime.strptime(file_date_str, '%Y-%m-%dT%H--%M--%S')
            
            # Рассчитать разницу в днях между текущей датой и датой файла
            days_difference = (current_date - file_date).days
            
            # Если файл старше, чем days_threshold, переместить его в архивную папку
            if days_difference > days_threshold:
                shutil.move(file_path, os.path.join(archive_folder, filename))
