from io import StringIO, BytesIO

from tempfile import TemporaryFile

import pandas as pd

import pymssql

import pyodbc
from sqlalchemy import text
import psycopg2
from psycopg2 import Error

from airflow.hooks.base import BaseHook

from clickhouse_driver import Client

import clickhouse_connect as cc

from minio import Minio, S3Error


class UpperQueryForDB:
    """ Класс для работы с базами данных
    """

    def __init__(self, connection_id: str) -> None:
        self._connection_id = connection_id
        # Подключаемся к бд
        self._connector_to_db = BaseHook.get_connection(self._connection_id)
        self._conn_type = self._connector_to_db.conn_type


    def __str__(self):

        return f"Данные по коннектору: {self._connector_to_db}\n\n Тип используемого подключения: {self._conn_type}"


    @staticmethod
    def get_connect_minio(con_id:str = 'S3_upper') -> Minio:
        """
        Он принимает идентификатор соединения в качестве входных данных и возвращает клиентский объект
        Minio.
        
        :param con_id: Имя соединения, которое вы создали в Airflow, defaults to S3_upper (optional)
        :return: Клиентский объект Minio
        """
        connection = BaseHook.get_connection(con_id)
        client = Minio(endpoint = connection.host, access_key=connection.login, secret_key=connection.password, secure=False)

        return client


    def get_postgres_connect(self):
        """
        Метод, который подключается к базе данных PostgreSQL.
        :return: connection (Объект соединения)
        """
        if self._conn_type != "postgres":
            raise Exception('Тип подключения не postgres')
        try:
            connection  = psycopg2.connect(host = self._connector_to_db.host, dbname = self._connector_to_db.schema, user = self._connector_to_db.login, password = self._connector_to_db.password)
            print("Информация о сервере PostgreSQL")
            print(connection.get_dsn_parameters(), "\n")
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
            raise error

        else:
            return connection


    def get_clickhouse_connect_cd(self):
        """
        Метод, который подключается к базе данных ClickHouse при помощи clickhouse_driver.
        :return: connection (Объект соединения)
        """
        if self._conn_type != "postgres":
            raise Exception('Тип подключения не postgres')

        try:
            connection = Client(host = self._connector_to_db.host,database=self._connector_to_db.schema,user=self._connector_to_db.login,password = self._connector_to_db.password, settings={'use_numpy': True})

        except (Exception) as error:
            print("Ошибка при работе с ClickHouse", error)
            raise error

        else:
            return connection


    def get_clickhouse_connect_cc(self):
        """
        Метод, который подключается к базе данных ClickHouse при помощи clickhouse_connect.
        :return: connection (Объект соединения)
        """
        if self._conn_type != "postgres":
            raise Exception('Тип подключения не postgres')

        try:
            connection = cc.get_client(host = self._connector_to_db.host,database=self._connector_to_db.schema,user=self._connector_to_db.login,password = self._connector_to_db.password)

        except (Exception) as error:
            print("Ошибка при работе с ClickHouse", error)
            raise error

        else:
            return connection
        

    def get_mssql_connect_pyodbc(self):
        """
        Метод, который подключается к базе данных MSSQL средствами библиотеки pyodbc.
        :return: connection (Объект соединения)
        """
        if self._conn_type != "mssql":
            raise Exception('Тип подключения не mssql')

        try:
            server = self._connector_to_db.host
            database = self._connector_to_db.schema
            username = self._connector_to_db.login
            password = self._connector_to_db.password
            connection = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+database+';ENCRYPT=yes;UID='+username+';PWD='+ password)

        except (Exception) as error:
            print("Ошибка при работе с MSSQL", error)
            raise error

        else:
            return connection


    def get_mssql_connect_pymssql(self):
        """
        Метод, который подключается к базе данных MSSQL средствами библиотеки pymssql.
        :return: connection (Объект соединения)
        """
        if self._conn_type != "mssql":
            raise Exception('Тип подключения не mssql')

        try:
            connection = pymssql.connect(host = self._connector_to_db.host, user = self._connector_to_db.login, password = self._connector_to_db.password, database = self._connector_to_db.schema)
        except (Exception) as error:
            print("Ошибка при работе с MSSQL", error)
            raise error

        else:
            return connection


    @classmethod
    def get_data_df_from_MSSQL(cls, query: str, connection) -> pd.DataFrame:
        """
        При выполнении запроса используем метод записи в словарь. 
        Далее читаем его пандами в датафрейм.
        
        :param cls: Класс, к которому принадлежит метод
        :param query: str - запрос, который нужно выполнить
        :type query: str
        :param connection: объект соединения
        :return: pd.DataFrame
        """
        try:
            cursor = connection.cursor(as_dict=True)
            cursor.execute(query)

            df = pd.DataFrame(cursor)

            return df
            
        except (Exception) as error:
            print("Ошибка при работе с MSSQL", error)
            raise error

        finally:
            if connection:
                cursor.close()
                connection.close()
                print("Соединение с MSSQL закрыто")
                

    @classmethod
    def get_data_df_from_PSQL_tmp(cls, query: str, connection) -> pd.DataFrame:
        """
        При выполнении запроса используем метод записи во временный файл на диск. Далее читаем его
        пандами в датафрейм.
        
        :param cls: класс создаваемого объекта
        :param query: str - запрос, который нужно выполнить
        :type query: str
        :param connection: подключение к базе данных
        :return: pd.DataFrame
        """

        with TemporaryFile() as tmpfile:
            # используем возможности бд PostgreSQL и производим выгрузку запроса сразу в csv файл
            try:
                copy_sql = f"COPY ({text(query)}) TO STDOUT WITH DELIMITER ';' CSV HEADER"
                cursor = connection.cursor()
                cursor.copy_expert(copy_sql, tmpfile)
                # возврат каретки в начало
                tmpfile.seek(0)
                # читаем в датафрейм
                df = pd.read_csv(tmpfile, sep=';')

                return df

            except (Exception, Error) as error:
                print("Ошибка при работе с PostgreSQL", error)
                raise error

            finally:
                if connection:
                    cursor.close()
                    connection.close()
                    print("Соединение с PostgreSQL закрыто")


    @classmethod
    def get_data_df_from_PSQL_sio(cls, query: str, connection) -> pd.DataFrame:
        """
        При выполнении запроса используем метод записи в объект StringIO, который сохраняет данные в
        памяти. Далее читаем его пандами в датафрейм.
        
        :param cls: Класс, к которому принадлежит метод
        :param query: str - запрос, который нужно выполнить
        :type query: str
        :param connection: объект соединения
        :return: pd.DataFrame
        """
        # используем возможности бд PostgreSQL и производим выгрузку запроса сразу в csv файл
        try:
            copy_sql = f"COPY ({text(query)}) TO STDOUT WITH DELIMITER ';' CSV HEADER"
            cursor = connection.cursor()
            # создаем строковый объект в памяти и помещаем в него результат запроса
            store = StringIO()
            cursor.copy_expert(copy_sql, store)
            # возврат каретки в начало
            store.seek(0)
            # читаем в датафрейм
            df = pd.read_csv(store, sep=';')

            return df

        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
            raise error

        finally:
            if connection:
                cursor.close()
                connection.close()
                print("Соединение с PostgreSQL закрыто")


    @classmethod
    def get_data_file_from_PSQL_to_minio(cls, query: str, connection, name_csv: str):
        """
        При выполнении запроса, используем метод записи во временный файл, далее преобразуем его в BytesIO объект и отправляет в хранилище.
        Метод удобен если нужно просто вычитать данные из PostgreSQL и потом залить в другую БД.
        Метод возвращает True если выполнен успешно.
        
        :param cls: Класс объекта, экземпляр которого создается
        :param query: SQL-запрос
        :type query: str
        :param connection: Объект соединения
        :param name_csv: str - имя файла который будет сохранен в хранилище
        :type name_csv: str
        :return: True
        """

        with TemporaryFile() as tmpfile:
            # используем возможности бд PostgreSQL и производим выгрузку запроса сразу в csv файл
            try:
                copy_sql = f"COPY ({text(query)}) TO STDOUT WITH DELIMITER ';' CSV HEADER"
                cursor = connection.cursor()
                cursor.copy_expert(copy_sql, tmpfile)
                tmpfile.seek(0)
                # читаем данные и получаем байт строку
                d = tmpfile.read()
                # создаем экземпляр BytesIO, который будет вести себя как файл, открытый в двоичном режиме.
                data=BytesIO(d)
                try:
                    UpperQueryForDB.get_connect_minio().put_object(
                        bucket_name='datalake',
                        object_name=f"{name_csv}.csv", 
                        length=len(d), # получаем количество байт
                        data=data, 
                        content_type = 'application/csv'
                    )
                except S3Error as err:
                    print(err)

                return True

            except (Exception, Error) as error:
                print("Ошибка при работе с PostgreSQL", error)
                raise error

            finally:
                if connection:
                    cursor.close()
                    connection.close()
                    print("Соединение с PostgreSQL закрыто")          


    @classmethod
    def load_data_to_postgresql(cls, connection, shema: str, table_name: str, create_table: str, name_csv: str, delete_table: str = None) -> bool:
        """ Метод:
             - получаем данные из хранилища
             - пишет в базу данных объект из хранилища minio(это файл csv).
             - создает таблицу, если она не существует, с переданными ей параметрами
             - удаляет таблицу по загружаемому периоду, если не None

             :param cls: Класс объекта, экземпляр которого создается
             :param connection: Объект соединения
             :param shema: str схема базы данных
             :type shema: str
             :param table_name: str название таблицы
             :type table_name: str
             :param create_table: str структура таблицы типа: название колоки тип данных колонки (branch_uuid uuid)
             :type create_table: str
             :param name_csv: str - имя файла который будет сохранен в хранилище
             :type name_csv: str
             :param delete_table: str строка удаления загружаемого периода, по умолчанию None
             :type delete_table: str

        """
        # получаем файл из корзины
        try:
            object_minio = UpperQueryForDB.get_connect_minio().get_object("datalake",f"{name_csv}.csv")
        except S3Error as err:
            print(err)

        # используем возможности бд PostgreSQL и производим запись в бд сразу файлом csv 
        try:
            copy_sql = f"COPY {shema}.{table_name} FROM STDIN WITH CSV DELIMITER ';' HEADER"

            # SQL-запрос для создания новой таблицы
            create_table_query = f'''CREATE TABLE IF NOT EXISTS {shema}.{table_name}
                            (
                             {create_table}
                            ); '''

            store = StringIO()
            # читаем файл из корзины и пишем его в объект StringIO
            store.write(object_minio.data.decode('utf-8'))
            store.seek(0)

            # создаем курсор для выполнения запросов к бд
            cursor = connection.cursor()

            # Выполнение команды: это создает новую таблицу
            cursor.execute(create_table_query)

            if delete_table is None:
                pass
            else:
                # Удаляем данные из таблицы по загружаемому периоду, чтобы не было дублей
                cursor.execute(delete_table)
            # используем метод copy_expert для записи файлов в бд
            cursor.copy_expert(copy_sql, store)

            # Сохраняем изменения
            connection.commit()

        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
            raise error

        finally:
            if connection:
                cursor.close()
                connection.close()
                print("Соединение с PostgreSQL закрыто")
            # удаляем загруженный файл из корзины
            try:
                UpperQueryForDB.get_connect_minio().remove_object("datalake",f"{name_csv}.csv")
            except S3Error as err:
                print(err)
                return True


    @classmethod
    def load_data_to_ClickHouse_CD(cls, connection, shema: str, table_name: str, create_table: str, name_csv: str, delete_table: str = None) -> bool:
        """ Метод:
             - пишет в базу данных объект из хранилища minio(это файл csv).
             - создает таблицу, если она не существует, с переданными ей параметрами
             - удаляет таблицу по загружаемому периоду, если не None
             - использует HTTPS протокол и библиотеку ClickHouse Connect Driver API

             :param cls: Класс объекта, экземпляр которого создается
             :param connection: Объект соединения
             :param shema: str схема базы данных
             :type shema: str
             :param table_name: str название таблицы
             :type table_name: str
             :param create_table: str структура таблицы типа: название колоки тип данных колонки (branch_uuid uuid)
             :type create_table: str
             :param name_csv: str - имя файла который будет сохранен в хранилище
             :type name_csv: str
             :param delete_table: str строка удаления загружаемого периода, по умолчанию None
             :type delete_table: str

        """
        # Получаем объект из хранилища преобразуем в датафрейм и загружаем в базу
        try:
            object_minio = UpperQueryForDB.get_connect_minio().get_object("datalake",f"{name_csv}.csv")
        except S3Error as err:
            print(err)

        try:
            # SQL-запрос для создания новой таблицы
            create_table_query = f"""CREATE TABLE IF NOT EXISTS {shema}.{table_name} {create_table}"""

            # Выполнение команды: это создает новую таблицу
            connection.command(create_table_query)

            if delete_table is None:
                pass
            else:
                # Удаляем данные из таблицы по загружаемому периоду, чтобы не было дублей
                connection.command(delete_table)

            connection.raw_insert(table = f"{shema}.{table_name}",insert_block = object_minio, settings = {'format_csv_delimiter':';'}, fmt = 'CSVWithNames')

        except (Exception) as error:

            print("Ошибка при работе с ClickHouse", error)
            raise error

        finally:
            # удаляем загруженный файл из корзины
            try:
                UpperQueryForDB.get_connect_minio().remove_object("datalake",f"{name_csv}.csv")
            except S3Error as err:
                print(err)


    @classmethod
    def load_data_to_click_house_cd(cls, connection, shema: str, table_name: str, name_csv: str, create_table: str, delete_table: str = None) -> bool:
        """ Метод:
             - пишет в базу данных объект из хранилища minio(это файл csv).
             - создает таблицу, если она не существует, с переданными ей параметрами
             - удаляет таблицу по загружаемому периоду, если не None
             - используем библиотеку clickhouse_driver и метод вставки в базу insert_dataframe

             :param cls: Класс объекта, экземпляр которого создается
             :param connection: Объект соединения
             :param shema: str схема базы данных
             :type shema: str
             :param table_name: str название таблицы
             :type table_name: str
             :param create_table: str структура таблицы типа: название колоки тип данных колонки (branch_uuid uuid)
             :type create_table: str
             :param name_csv: str - имя файла который будет сохранен в хранилище
             :type name_csv: str
             :param delete_table: str строка удаления загружаемого периода, по умолчанию None
             :type delete_table: str
        """
        # Получаем объект из хранилища преобразуем в датафрейм и загружаем в базу
        try:
            object_minio = UpperQueryForDB.get_connect_minio().get_object("datalake",f"{name_csv}.csv")
        except S3Error as err:
            print(err)

        try:
            df = pd.read_csv(object_minio, sep=';')

            # SQL-запрос для создания новой таблицы
            create_table_query = f"""CREATE TABLE IF NOT EXISTS {shema}.{table_name} {create_table}"""

            # Выполнение команды: это создает новую таблицу
            connection.execute(create_table_query)

            if delete_table is None:
                pass
            else:
                # Удаляем данные из таблицы по загружаемому периоду, чтобы не было дублей
                connection.execute(delete_table)

            connection.insert_dataframe(f'INSERT INTO {shema}.{table_name} VALUES',df)

        except (Exception) as error:
            print("Ошибка при работе с ClickHouse", error)
            raise error

        finally:
            if connection:
                connection.disconnect()
                print("Соединение с ClickHouse закрыто")
            # удаляем загруженный файл из корзины
            try:
                UpperQueryForDB.get_connect_minio().remove_object("datalake",f"{name_csv}.csv")
            except S3Error as err:
                print(err)
            return True
