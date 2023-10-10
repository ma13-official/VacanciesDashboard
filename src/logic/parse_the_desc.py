#!/usr/bin/env python
import json
import re
import sys
# sys.path.append('/home/collector/VacanciesDashboard/src')
from time import perf_counter as pc
from logic.json_sql_loader import JSONSQLDownloader
from logic.logger import Logger
from progress.bar import IncrementalBar



class Connecter:
    dataset = []

    def parse_the_csv():
        csv_filename = "/home/collector/VacanciesDashboard/skills_sorted.csv"
        result_list = []

        with open(csv_filename, "r", encoding='latin-1') as csv_file:
            for row in csv_file.readlines():
                if row[-1] == '\n':
                    result_list.append(row[:-1])
                else:
                    result_list.append(row)
                
        result_list[1] = 'форсайт'

        return result_list

    def parse_skills(skills = JSONSQLDownloader.get_all_from_skills()):

        def find_letter_without_latin_neighbors(input_string, letter):
            # Используем регулярное выражение для поиска "c" без латинских соседей
            if letter == 'c':
                pattern = rf'(?:[^a-zA-Z1]|^){letter}(?:[^a-zA-Z+#]|$)'
            else:
                pattern = rf'(?:[^a-zA-Z]|^){letter}(?:[^a-zA-Z]|$)'
            
            # Ищем все совпадения в строке
            matches = re.finditer(pattern, input_string)
            
            if len(list(matches)) != 0:
                return letter

        JSONSQLDownloader.connect_to_db()  # connect_to_db there too
        Logger.warning(f'Skills {len(skills)}')

        # keywords = Connecter.extract_values('awesome.json')
        keywords = Connecter.parse_the_csv()
        Logger.warning(f'Keywords {len(keywords)}')

        bar = IncrementalBar('Parsing', max = len(skills))

        count = 0
        for skill in skills:

            count += 1
            if count % 1000 == 0:
                Logger.warning(skill)
            elif count % 100 == 0:
                Logger.info(skill)

            bar.next()
            if JSONSQLDownloader.check_existence('skills_clear_test', skill[0]):
               continue 
        
            arrays = []

            for keyword in keywords:
                if keyword in ['c', 'r']:
                    res = find_letter_without_latin_neighbors(skill[1], keyword)
                    if res is not None:
                        arrays.append(res)
                elif keyword == 'c#':
                    arrays.append(Parser.kmp_search(keyword, skill[1]))
                    arrays.append(Parser.kmp_search('c #', skill[1]))
                    arrays.append(Parser.kmp_search('sharp', skill[1]))
                elif keyword == 'java':
                    if Parser.kmp_search('javascript', skill[1]) == []:
                        arrays.append(Parser.kmp_search('java', skill[1]))
                elif keyword == '1c':
                    arrays.append(Parser.kmp_search(keyword, skill[1]))
                    arrays.append(Parser.kmp_search('1с', skill[1]))
                else:
                    arrays.append(Parser.kmp_search(keyword, skill[1]))

            def merge_arrays(arrays):
                return sorted(set([item for array in arrays for item in array]))

            arrays = merge_arrays(arrays)

            for i in arrays:
                if [skill[0], i] not in Connecter.dataset:
                    Connecter.dataset.append([skill[0], i, skill[1]])

        Connecter.save(Connecter.dataset)
        Connecter.update_skills_clean()
        Logger.warning('Skills parsed')

    def update_skills_clean():
        # with open('123.json', 'r', encoding='utf-8') as f:
        #     Connecter.dataset = json.load(f)

        # JSONSQLDownloader.connect_to_db()

        print()
        bar = IncrementalBar('Deleting', max = len(Connecter.dataset))
        for row in Connecter.dataset:
            if JSONSQLDownloader.check_existence('skills_clear_test', row[0]):
               JSONSQLDownloader.delete_by_id('skills_clear_test', row[0])
            bar.next()

        print()
        bar = IncrementalBar('Inserting', max = len(Connecter.dataset))
        for row in Connecter.dataset:
            if row[1] == '1с':
                JSONSQLDownloader.insert_sct([row[0], '1c'])
            else:
                JSONSQLDownloader.insert_sct(row[0:2])
            bar.next()

    def extract_values(json_file):
        with open(json_file, 'r') as file:
            data = json.load(file)

        result = []
        def extract(obj):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    extract(v)
            elif isinstance(obj, list):
                for item in obj:
                    extract(item)
            else:
                result.append(obj)

        extract(data)
        return result

    def save(arr):
        with open('123.json', 'w', encoding='utf-8') as f:
            json.dump(arr, f, indent=4, ensure_ascii=False)


class Parser:
    @staticmethod
    def kmp_search(word, string, max_diff=1):
        word = word.lower()
        string = string.lower()
        n = len(string)
        m = len(word)
        lps = [0] * m
        j = 0

        def computeLPSArray(word, m, lps):
            length = 0
            lps[0] = 0
            i = 1
            while i < m:
                if word[i] == word[length]:
                    length += 1
                    lps[i] = length
                    i += 1
                else:
                    if length != 0:
                        length = lps[length - 1]
                    else:
                        lps[i] = 0
                        i += 1

        computeLPSArray(word, m, lps)

        i = 0
        result = []
        diff_count = 0  # Счетчик различающихся символов
        while i < n:
            if word[j] == string[i]:
                i += 1
                j += 1
            else:
                if j != 0:
                    j = lps[j - 1]
                else:
                    i += 1
                diff_count += 1  # Увеличение счетчика различающихся символов

            if j == m:
                if diff_count <= max_diff:
                    result.append(string[i - j:i])
                j = lps[j - 1]
                diff_count = 0  # Сброс счетчика различающихся символов

        return result


def main():
    start = pc()
    Connecter.parse_skills()
    # Connecter.update_skills_clean()
    print()
    print(pc()-start)


if __name__ == "__main__":
    main()