from datetime import timedelta

from pendulum import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import BranchPythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator


default_args = {'owner': 'Kolokolov.KA',
                'start_date': datetime(2022, 2, 13, tz='Europe/Moscow'),
                'email': ['******'],
                'email_on_failure': True,
                'email_on_retry': False,
                "retries": 1,
                'retry_delay': timedelta(minutes=1)
                }


dag = DAG('dim_branch_new', 
          schedule_interval='@weekly',
          description='Иерархия территорий из DWH в Postgres (upper_volga.dim_branch)',
          default_args=default_args,
          tags=['справочник', 'справочник филиалов'],
          max_active_runs=1,
          catchup=False)


def time() -> str:
    '''
    Эта функция возвращает текущую дату в формате ГГГГ-ММ-ДД.
     :return: дата в формате ГГГГ-ММ-ДД.
    '''
    from pendulum import now

    start_date = now()
    start_date = f"{start_date:%d-%m-%Y}"

    return start_date


def query_generate() -> str:
    """
     :return: Запрос возвращается.
    """
    from os.path import abspath, dirname, join

    base_path = dirname(abspath(__file__))

    files_path_pdf = join(base_path, 'sql', 'query.sql')
    with open(files_path_pdf, 'r', encoding='utf-8') as file:

        temp_query = file.read()

    query = temp_query

    return query


def extract_data(name_csv: str, **context):
    """
    Функция обращается к БД и формирует DataFrame с данными.
    Возвращаяет - DataFrame
    """
    from io import BytesIO
    from minio.error import S3Error
    from methods_work_db import UpperQueryForDB

    query = context['task_instance'].xcom_pull('query_generate')

    # Подключаемся к бд
    obj = UpperQueryForDB('ms_dwh')

    df_dwh = obj.get_data_df_from_MSSQL(query, obj.get_mssql_connect_pymssql())

    if df_dwh.empty:

        return 'tg_empty'

    else:
        # Превращаем датафрейм в csv и отправляем в хранилище
        csv = df_dwh.to_csv(sep=';', index=False).encode('utf-8')
        try:
            UpperQueryForDB.get_connect_minio().put_object(
                bucket_name='datalake',
                object_name=f"{name_csv}.csv",
                length=len(csv),  # Получить количество байт
                # Создайте экземпляр BytesIO, который будет вести себя как файл, открытый в двоичном режиме.
                data=BytesIO(csv),
                content_type='application/csv'
            )
        except S3Error as err:
            print(err)

        return True


def select_next_task(**context) -> str:
    """
     Если кадр данных пуст, верните 'tg_df_empty', иначе верните 'next_task_id'
      :return: Имя следующей выполняемой задачи.
    """

    flag = context['task_instance'].xcom_pull('extract_data')

    if flag == 'tg_empty':

        return 'send_message_telegram_df_empty'
    else:

        return 'load_data'


def load_data(shema: str, table_name: str, create_table: str, name_csv: str):
    """
    Функция записывает данные из DWH в PostrgeSql
    :return: True
    """
    from methods_work_db import UpperQueryForDB

    # Подключаемся к бд
    client = UpperQueryForDB('dwh_upper_volga')
    connection = client.get_postgres_connect()

    # Удаляем данные из таблицы
    delete_table = f"""truncate TABLE {shema}.{table_name}"""
    
    # вставляем данные
    client.load_data_to_postgresql(connection, shema, table_name, create_table, name_csv, delete_table)

    return True


_query_generate = PythonOperator(task_id='query_generate',
                                 provide_context=True,
                                 python_callable=query_generate,
                                 dag=dag)


_extract_data = PythonOperator(task_id='extract_data',
                               provide_context=True,
                               python_callable=extract_data,
                               op_kwargs={'name_csv':'{{ dag.dag_id }}'},
                               dag=dag)


_select_next_task = BranchPythonOperator(task_id='select_next_task',
                                         provide_context=True,
                                         python_callable=select_next_task,
                                         dag=dag)


_load_data = PythonOperator(task_id='load_data',
                            provide_context=True,
                            python_callable=load_data,
                            op_kwargs = {'shema': '******',
                                         'table_name': 'dim_branch',
                                         'name_csv':'{{ dag.dag_id }}',
                                         'create_table': """ 
                                                            (id_branch int,
                                                            code_branch numeric,
                                                            source_id uuid,
                                                            city_source_id uuid,
                                                            branch_name varchar(50),
                                                            fk_division int,
                                                            code_division varchar(7),
                                                            division_name varchar(25),
                                                            fk_rrs_lvl_1 int,
                                                            rrs_lvl_1_name varchar(35),
                                                            fk_rrs_lvl_2 int,
                                                            rrs_lvl_2_name varchar(50),
                                                            date_open date,
                                                            date_close date,
                                                            branch_type varchar(35),
                                                            fk_city int,
                                                            city_name varchar(50),
                                                            fk_region int,
                                                            region_name varchar(50),
                                                            is_open varchar(12),
                                                            total_area float,
                                                            sales_area float,
                                                            rental_rate float,
                                                            code_branch numeric,
                                                            source_id uuid,
                                                            city_source_id uuid,
                                                            code_division varchar(7),
                                                            rrs_lvl_1_code varchar(7),
                                                            rrs_lvl_2_code varchar(7),
                                                            format varchar(50),
                                                            CONSTRAINT dim_branch_pk PRIMARY KEY (id_branch)) """
                                                            },
                            dag=dag)

# Задача срабатывает, при условии, что df пуст
_tg_df_empty = TelegramOperator(
    task_id='send_message_telegram_df_empty',
    telegram_conn_id='telegram_conn_id',
    chat_id='*******',
    text="""DAG '{{ dag.dag_id }}' вернул пустой датафрейм. Автор дага: {{dag.owner}}, дата запуска: {{params.start_date}}.""",
    dag=dag,
    params={'start_date': time(), }
)

# Задача срабатывает, при условии, что любой таск завершается с ошибкой
_tg_failed = TelegramOperator(
    task_id='send_message_telegram_failed',
    telegram_conn_id='telegram_conn_id',
    chat_id='********',
    text="""Даг '{{ dag.dag_id }}' упал или выполнился с ошибкой. Автор дага: {{dag.owner}}.""",
    dag=dag,
    trigger_rule=TriggerRule.ONE_FAILED
)

_query_generate >> _extract_data >> _select_next_task >> [_tg_df_empty, _load_data]
_load_data >> _tg_failed
_tg_df_empty >> _tg_failed
