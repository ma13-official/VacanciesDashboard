import psycopg2
from psycopg2 import Error
import json
import re
from datetime import datetime
from dotenv import dotenv_values
from settings.config import Local 
from logic.logger import Logger


class JSONSQLDownloader:
    prefix = "hh.ru/vacancies_jsons/"
    drop = """
    DROP TABLE IF EXISTS vacancy;
    """

    create = """
    CREATE TABLE IF NOT EXISTS vacancy (
        hhid INT PRIMARY KEY NOT NULL,
        premium BOOLEAN DEFAULT FALSE,
        billing_type JSONB,
        relations JSONB,
        relations_size INT,
        name TEXT,
        insider_interview JSONB,
        response_letter_required BOOLEAN DEFAULT FALSE,
        area JSONB,
        salary JSONB,
        type JSONB,
        address JSONB,
        allow_messages BOOLEAN DEFAULT FALSE, 
        experience JSONB,
        schedule JSONB,
        employment JSONB,
        department JSONB,
        contacts JSONB,
        description TEXT, 
        branded_description JSONB, 
        vacancy_constructor_template JSONB, 
        key_skills JSONB,
        key_skills_size INT,
        accept_handicapped BOOLEAN DEFAULT FALSE, 
        accept_kids BOOLEAN DEFAULT FALSE,
        archived BOOLEAN DEFAULT FALSE,
        response_url JSONB,
        specializations JSONB,
        specializations_size INT,
        professional_roles JSONB,
        professional_roles_size INT,
        code JSONB,
        hidden BOOLEAN DEFAULT FALSE,
        quick_responses_allowed BOOLEAN DEFAULT FALSE,
        driver_license_types JSONB,
        driver_license_types_size INT,
        accept_incomplete_resumes BOOLEAN DEFAULT FALSE, 
        employer JSONB,
        published_at TIMESTAMP, 
        created_at TIMESTAMP,
        initial_created_at TIMESTAMP,
        negotiations_url JSONB,
        suitable_resumes_url JSONB,
        apply_alternate_url TEXT,
        has_test BOOLEAN DEFAULT FALSE,
        test JSONB,
        alternate_url TEXT,
        working_days JSONB,
        working_days_size INT,
        working_time_intervals JSONB,
        working_time_intervals_size INT,
        working_time_modes JSONB,
        working_time_modes_size INT,
        accept_temporary BOOLEAN DEFAULT FALSE,
        languages JSONB,
        languages_size INT
        );
    """

    columns = ["hhid",
               "premium",
               "billing_type",
               "relations",  # JSON ARRAY
               "relations_size",
               "name",
               "insider_interview",
               "response_letter_required",
               "area",
               "salary",
               "type",
               "address",
               "allow_messages",
               "experience",
               "schedule",
               "employment",
               "department",
               "contacts",
               "description",
               "branded_description",
               "vacancy_constructor_template",
               "key_skills",
               "key_skills_size",
               "accept_handicapped",
               "accept_kids",
               "archived",
               "response_url",
               "specializations",  # JSON ARRAY
               "specializations_size",
               "professional_roles",  # JSON ARRAY
               "professional_roles_size",
               "code",
               "hidden",
               "quick_responses_allowed",
               "driver_license_types",  # JSON ARRAY
               "driver_license_types_size",
               "accept_incomplete_resumes",
               "employer",
               "published_at",
               "created_at",
               "initial_created_at",
               "negotiations_url",
               "suitable_resumes_url",
               "apply_alternate_url",
               "has_test",
               "test",
               "alternate_url",
               "working_days",  # JSON ARRAY
               "working_days_size",
               "working_time_intervals",  # JSON ARRAY
               "working_time_intervals_size",
               "working_time_modes",  # JSON ARRAY
               "working_time_modes_size",
               "accept_temporary",
               "languages",  # JSON ARRAY
               "languages_size"
               ]
    clmn_sql = tuple(columns)

    records_list_template = '(' + ','.join(['%s'] * len(columns)) + ')'
    insert_query = 'INSERT INTO vacancy VALUES {}'.format(records_list_template)

    @classmethod
    def connect_to_db(cls):
        try:
            # Connect to an existing database
            cls.connection = psycopg2.connect(user=dotenv_values(Local.env)["DB_NAME"],
                                              password=dotenv_values(Local.env)["DB_PASSWORD"],
                                              host=dotenv_values(Local.env)["DB_HOST"],
                                              port=dotenv_values(Local.env)["DB_PORT"],
                                              database=dotenv_values(Local.env)["DB"])

            # Create a cursor to perform database operations
            cursor = cls.connection.cursor()
            # Print PostgreSQL details
            print("PostgreSQL server information")
            print(cls.connection.get_dsn_parameters())
            # Executing a SQL query
            cursor.execute("SELECT version();")
            cls.connection.commit()
            print("You are connected!")

            print()

        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)

    @classmethod
    def check_existence(cls, table_name, id):
        cursor = cls.connection.cursor()
        cls.connection.commit()

        cursor.execute(f"SELECT * FROM {table_name} WHERE hhid = {id}")
        row = cursor.fetchone()

        return row is not None

    @classmethod
    def delete_by_id(cls, table_name, id):
        cursor = cls.connection.cursor()
        cls.connection.commit()

        cursor.execute(f"DELETE FROM {table_name} WHERE hhid = {id}")
        cls.connection.commit()

    @classmethod
    def insert_sct(cls, vals):
        cursor = cls.connection.cursor()
        cls.connection.commit()

        cursor.execute(f"SELECT * FROM skills_clear_test WHERE hhid = {vals[0]} and skill = '{vals[1]}'")
        row = cursor.fetchone()

        if row is None:
            records_list_template = '(' + ','.join(['%s'] * len(vals)) + ')'
            insert_query = 'INSERT INTO skills_clear_test VALUES {}'.format(records_list_template)

            cursor.execute(insert_query, vals)
            cls.connection.commit()

    def prep_vals(content):
        res = []
        bool_vals = ['premium', 'response_letter_required',
                     'allow_messages', 'accept_handicapped',
                     'accept_kids', 'archived', 'hidden',
                     'quick_responses_allowed', 'accept_incomplete_resumes',
                     'has_test', 'accept_temporary']
        int_vals = ['id']
        text_vals = ['description', 'apply_alternate_url', 'alternate_url', 'name']
        json_vals = ['billing_type',
                     'relations',
                     'insider_interview',
                     'area',
                     'salary',
                     'type',
                     'address',
                     'experience',
                     'schedule',
                     'employment',
                     'department',
                     'contacts',
                     'branded_description',
                     'vacancy_constructor_template',
                     'key_skills',
                     'response_url',
                     'specializations',
                     'professional_roles',
                     'code',
                     'driver_license_types',
                     'employer',
                     'negotiations_url',
                     'suitable_resumes_url',
                     'test',
                     'working_days',
                     'working_time_intervals',
                     'working_time_modes',
                     'languages']
        date_vals = ['published_at', 'created_at', 'initial_created_at']
        date_format = '%Y-%m-%dT%H:%M:%S%z'

        for key, val in content.items():
            if key in int_vals:
                new_type = int(val)
                res.append(new_type)
            if key in bool_vals:
                new_type = bool(val)
                res.append(new_type)
            if key in text_vals:
                if key == 'description':
                    new_type = str(re.sub('<[^<]+?>', '', str(val)).replace('&quot', ''))
                else:
                    new_type = str(val)
                res.append(new_type)
            if key in json_vals:
                if val == None:
                    res.append(val)
                if val == []:
                    res.append(val)
                    res.append(0)
                if val != [] and val != None:
                    if type(val) == list:
                        new_type = json.dumps(val)
                        res.append(new_type)
                        res.append(len(val))
                    else:
                        new_type = json.dumps(val)
                        res.append(new_type)
            if key in date_vals:
                new_type = datetime.strptime(val, date_format)
                res.append(new_type)

        return res

    @classmethod
    def upload_file_to_db(cls, path):
        with open(path, 'r', encoding='utf-8') as f:
            try:   
                json_content = json.load(f)
            except Exception as e:
                print(e)
                print(path)
                input()

        cursor = cls.connection.cursor()
        cls.connection.commit()

        vals = cls.prep_vals(json_content)

        try:
            if not cls.check_existence('vacancy', json_content['id']):
                cursor.execute(cls.insert_query, vals)
        except KeyError:
            Logger.error(f"Wrong JSON {json_content}")
        except Exception as e:
            Logger.error(f"Unexcepted error {e}")

    @classmethod
    def update_mv(cls):
        cursor = cls.connection.cursor()

        for mw in ['mw1', 'skills', 'specs']:

            cursor.execute(f"REFRESH MATERIALIZED VIEW {mw}")
            cursor.execute(f"SELECT count(*) FROM {mw}")
            row = cursor.fetchone()
            Logger.warning(f"{mw} refreshed, {row[0]} rows in material view now.")

        cls.connection.commit()
        
    @classmethod
    def update_active(cls, arr):
        cls.connect_to_db()
        cursor = cls.connection.cursor()

        try:            
            # Сначала установим значение FALSE для всех строк
            update_all_query = "UPDATE vacancy SET not_archived = FALSE WHERE not_archived = TRUE"
            cursor.execute(update_all_query)
            cls.connection.commit()
            
            Logger.warning("Значения во всех строках обновлены на FALSE.")
            
            # Проходимся по массиву ID и устанавливаем значение TRUE для каждой строки
            total = 0
            for row_id in arr:
                total += 1
                update_query = f"UPDATE vacancy SET not_archived = TRUE WHERE hhid = {row_id}"
                cursor.execute(update_query)
                cls.connection.commit()
                if total % 10000 == 0:
                    Logger.warning(f"{total} Значение в строке с ID {row_id} обновлено на TRUE.")
                else:
                    Logger.info(f"{total} Значение в строке с ID {row_id} обновлено на TRUE.")

            Logger.warning(f"{total} Значение в строке с ID {row_id} обновлено на TRUE.")

            #   'vacancies_active', 'skills_active', 'specs_active', 
            #   не обновляем эти view, поскольку они не используются
            
            for mw in ['archived_vacancy_status']:
                cursor.execute(f"REFRESH MATERIALIZED VIEW {mw}")
                cursor.execute(f"SELECT count(*) FROM {mw}")
                row = cursor.fetchone()
                Logger.warning(f"{mw} refreshed, {row[0]} rows in material view now.")
            
        except (Exception, psycopg2.Error) as error:
            Logger.error("Ошибка при работе с PostgreSQL:" + str(error))
            
        finally:
            # Закрытие курсора и соединения
            if cursor:
                cursor.close()
            if cls.connection:
                cls.connection.commit()
                cls.connection.close()

    @classmethod
    def get_all_from_skills_active(cls):
        cls.connect_to_db()
        cursor = cls.connection.cursor()

        cursor.execute('SELECT * FROM skills_active')
        rows = cursor.fetchall()

        return rows

    @classmethod
    def get_all_from_skills(cls):
        cls.connect_to_db()
        cursor = cls.connection.cursor()

        cursor.execute('SELECT * FROM skills')
        rows = cursor.fetchall()

        return rows

    @classmethod
    def get_vacancy_names(cls):
        cls.connect_to_db()
        cls.cursor = cls.connection.cursor()

        cls.cursor.execute('SELECT id_, vacancy_name_ FROM mw1')
        rows = cls.cursor.fetchall()

        return rows

    @classmethod
    def update_vacancy_name(cls, name):
        query = f"UPDATE mw1 SET vacancy_name_ = '{name[2]}' WHERE id_ = {name[0]}"
        cls.cursor.execute(query)
        cls.connection.commit()