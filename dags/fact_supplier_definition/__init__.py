from typing import Union

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python import BranchPythonOperator
from airflow.hooks.base import BaseHook

from pendulum import datetime

from sqlalchemy import Numeric, Integer, String, DateTime, create_engine
from sqlalchemy.dialects.postgresql import UUID


default_args = {'owner': 'Kolokolov.KA',
                'start_date': datetime(2022, 12, 23,tz='Europe/Moscow'),
                'email': ['***'],
                'email_on_failure': True
                }


dag = DAG('fact_supplier_definition', 
          schedule_interval='0 8-19 * * *',
          description='Определение поставщика',
          default_args=default_args,
          tags=['GP'],
          catchup=False)


def connect_to_api(to_api, headers, URL)->list:
    """Функция, которая подключается к API и возвращает список словарей."""
    import json as js
    import requests

    list_sup_local: list = []

    try:
        with requests.Session() as s:
            response = s.get(URL, headers=headers, json=to_api)
            response.raise_for_status()  # raises exception when not a 2xx response
            if response.status_code != 204:
                data1 = response.text
                # data:list[dict[str,str]] = response.json()
            try:
                data = js.loads(data1)
                for i in range(len(data)):
                    list_sup_local.append({'nomenclatureGuid':data[i]['NomenclatureID'],'SN':data[i]['SN'],'SupplierID':data[i]['SupplierID']})

                return list_sup_local
            except js.JSONDecodeError:
                print("Empty response")
                       
    except requests.HTTPError as e:
        print (f"OОпа!  Что за ошибка: {e}.")

    
def time_tg() -> str:
    """ Функция формирует дату запуска дага, для использования в репорте телеграмма"""

    from pendulum import now

    start_date = now(tz='Europe/Moscow')
    start_date = str(start_date)[0:4] + '-' + \
        str(start_date)[5:7] + '-' + str(start_date)[8:10]
    return start_date


def query_generate(file_name: str) -> str:
    """
      Берем дату из предыдущей задачи и использует ее для генерации запроса.
     :return: Запрос возвращается.
    """
    from os.path import abspath, dirname, join

    base_path = dirname(abspath(__file__))

    files_path_pdf = join(base_path,'sql', file_name)
    with open(files_path_pdf, 'r', encoding='utf-8') as file:
        temp_query = file.read()

    query = temp_query
    
    return query
        

def extract_data(name_csv: str,task_id: str,**context) -> Union[str,bool]:
    
    from io import BytesIO
    from minio.error import S3Error
    from methods_work_db import UpperQueryForDB

    query = context['task_instance'].xcom_pull(task_id)

    # Подключаемся к бд
    obj = UpperQueryForDB('green_plum')

    df_green = obj.get_data_df_from_PSQL_tmp(query, obj.get_postgres_connect())
    
    if df_green.empty:

        return 'tg_empty'

    else:
        # Превращаем датафрейм в csv и отправляем в хранилище
        csv = df_green.to_csv(sep = ';',index=False).encode('utf-8')
        try:
            UpperQueryForDB.get_connect_minio().put_object(
                bucket_name='datalake',
                object_name=f"{name_csv}.csv", 
                length=len(csv), # Получить количество байт
                data=BytesIO(csv), # Создайте экземпляр BytesIO, который будет вести себя как файл, открытый в двоичном режиме.
                content_type = 'application/csv'
            )
        except S3Error as err:
            print(err)

        return True


def select_next_task(task_id: str,next_task_id: str,**context) -> str:
    """
    Если кадр данных пуст, верните 'tg_df_empty', иначе верните 'next_task_id'
     :return: Имя следующей выполняемой задачи.
    """

    flag = context['task_instance'].xcom_pull(task_id)
    

    if flag == 'tg_empty':

        return 'tg_df_empty'
    else:
        return next_task_id


def transform_data(name_csv: str) -> bool:

    import time
    from io import BytesIO
    from minio.error import S3Error
    from methods_work_db import UpperQueryForDB
    from pandas import read_csv, DataFrame, merge

    # Получаем объект из хранилища преобразуем в датафрейм 
    try:
        obj = UpperQueryForDB.get_connect_minio().get_object("datalake",f"{name_csv}.csv")
        
    except S3Error as err:
        print(err)

    try:
        UpperQueryForDB.get_connect_minio().remove_object("datalake",f"{name_csv}.csv")

    except S3Error as err:
        print(err)

    df_green = read_csv(obj, sep=';')

    # отбираем колонки для запроса в API
    df_input = df_green[['nomenclatureguid','serialnumber']].dropna(subset=['serialnumber']).drop_duplicates().reset_index(drop=True)

    # переименуем в соответствии с шаблоном запроса
    df_input.rename(columns={'nomenclatureguid':'nomenclatureGuid','serialnumber':'serialNumber'},inplace=True)

    # uuid переведем в строку
    df_input=df_input.assign(nomenclatureGuid=df_input.nomenclatureGuid.astype(str))

    # находим число строк во фрейме
    len_df_input: int = df_input.shape[0]
    list_sup: list = []

    connection = BaseHook.get_connection('web_sn_data_API')

    headers = {
    'Authorization': f"Basic {connection.password}", 'Content-Type': 'application/json', 'Accept': 'application/json'
    }
    URL:str = connection.host

    # посылаем запрос через каждые 6сек по 100 записей если строк больше 100
    if len_df_input > 100:
        # делим фрейм по 100 записей, формируем объект json и отправляем запрос. делаем перерыв между запросами 2 сек
        for index in range(100,len_df_input + 100,100):
            df = df_input.iloc[index-100:index]
            to_api = {'requested_products':df.to_dict('records')}
            _ = connect_to_api(to_api,headers,URL)
            if _ == None:
                print('None')
                continue
            # while _ == None:
            #     time.sleep(10)
            #     _ = connect_to_api(to_api,headers,URL)
            #     print('None')
            # print(_)
            list_sup.extend(_)
            # print('Перерыв')
            time.sleep(6)
    else:
        to_api = {'requested_products':df_input.to_dict('records')}
        list_sup.extend(connect_to_api(to_api))

    # превращаем список со словарями в датафрейм 
    df_out = DataFrame(list_sup)

    # соединяем полученный фрейм df_out с основным запросом df_green
    df_finish = merge(df_green.assign(nomenclatureguid=df_green.nomenclatureguid.astype(str)),df_out.assign(nomenclatureGuid=df_out.nomenclatureGuid.astype(str)), how='left',left_on=['serialnumber',   'nomenclatureguid'],right_on=["SN",'nomenclatureGuid'])  

    # удаляем и переименовываем лишние колонки
    df_finish = df_finish.drop(columns=['nomenclatureGuid','SN']).rename(columns={'SupplierID':'supplier_uuid'})
    df_finish = df_finish[['load_date','product', 'nomenclatureguid', 'serialnumber', 'code', 'doc_op', 'date_doc_op' ,'doc_sog', 'date_doc_sog', 'time_since_last_status',  'sum_with_out_vat', 'supplier_uuid', 'storehouse', 'date_osnt']]
    df_finish['time_since_last_status'] = df_finish['time_since_last_status'].astype('Int64')

    # Превращаем датафрейм в csv и отправляем в хранилище
    csv = df_finish.to_csv(sep = ';',index=False).encode('utf-8')
    try:
        UpperQueryForDB.get_connect_minio().put_object(
            bucket_name='datalake',
            object_name=f"{name_csv}.csv", 
            length=len(csv), # Получить количество байт
            data=BytesIO(csv), # Создайте экземпляр BytesIO, который будет вести себя как файл, открытый в двоичном режиме.
            content_type = 'application/csv'
        )
    except S3Error as err:
        print(err)

    return True


def load_data(shema: str,table_name: str,create_table: str, table_data_type: str, name_csv: str) -> bool:

    from minio.error import S3Error
    from methods_work_db import UpperQueryForDB
    from pandas import read_csv
    from db_resources import get_postgres_connection_string

    # Получаем объект из хранилища преобразуем в датафрейм и загружаем в базу
    try:
        obj = UpperQueryForDB.get_connect_minio().get_object("datalake",f"{name_csv}.csv")
    except S3Error as err:
        print(err)

    df = read_csv(obj, sep=';')

    # Подключаемся к бд
    engine = create_engine(get_postgres_connection_string("dwh_upper_volga"))
    conn = engine.connect()
    transaction = conn.begin()

    # SQL-запрос для создания новой таблицы
    create_table_query = f'''CREATE TABLE IF NOT EXISTS {shema}.{table_name}
                            (
                             {create_table}
                            ); '''
    # Выполнение команды: это создает новую таблицу
    engine.execute(create_table_query)

    # Удаляем данные из таблицы по загружаемому периоду, чтобы не было дублей
    delete_table = f"""
        truncate TABLE {shema}.{table_name}
        """
    engine.execute(delete_table)

    # вставляем данные
    # client.load_data_to_postgresql(connection, obj, shema, table_name, create_table, delete_table)

    df.to_sql(table_name, con=conn, schema=shema, method='multi', if_exists='append', index=False, chunksize=100000, dtype=table_data_type)
    # Сохраняем изменения
    transaction.commit()
    # Закрывает соединения с базой данных в который будем осуществлять запись
    engine.dispose()

    try:
        UpperQueryForDB.get_connect_minio().remove_object("datalake",f"{name_csv}.csv")

    except S3Error as err:
        print(err)

    return True    



_query_generate = PythonOperator(task_id='query_generate',
                                 provide_context=True,
                                 python_callable=query_generate,
                                 op_kwargs={'file_name':'query.sql'},
                                 dag=dag)


_extract_data = PythonOperator(task_id='extract_data',
                               provide_context=True,
                               python_callable=extract_data,
                               pool = 'load_1C_file',
                               op_kwargs={'name_csv':'{{ dag.dag_id }}',
                               'task_id':'query_generate'},
                               dag=dag)


_select_next_task = BranchPythonOperator(task_id='select_next_task',
                                        provide_context=True,
                                        python_callable=select_next_task,
                                        op_kwargs={'task_id':'extract_data',
                                        'next_task_id':'transform_data'},
                                        dag=dag)


_transform_data = PythonOperator(task_id='transform_data',
                               provide_context=True,
                               python_callable=transform_data,
                               pool = 'load_1C_file',
                               op_kwargs={'name_csv':'{{ dag.dag_id }}'},
                               dag=dag)


_load_data = PythonOperator(task_id='load_data',
                                       provide_context=True,
                                       python_callable=load_data,
                                       pool = 'load_1C_file',
                                       op_kwargs={'shema' : 'upper_volga',
                                                  'table_name': 'fact_supplier_definition',
                                                  'create_table':"""
                                                                    load_date timestamp,
                                                                    product varchar(150),
                                                                    nomenclatureguid uuid,
                                                                    serialnumber text,
                                                                    code varchar(7),
                                                                    doc_op text,
                                                                    date_doc_op timestamp,
                                                                    doc_sog text,
                                                                    date_doc_sog timestamp,
                                                                    time_since_last_status int,
                                                                    sum_with_out_vat numeric,
                                                                    supplier_uuid uuid,
                                                                    storehouse varchar(80),
                                                                    date_osnt timestamp
                                                                    """,
                                                    'table_data_type':{  'load_date': DateTime()
                                                                        , 'product': String
                                                                        , 'nomenclatureguid': UUID(as_uuid=True)
                                                                        , 'serialnumber': String
                                                                        , 'code':String
                                                                        , 'doc_op':String
                                                                        , 'date_doc_op':DateTime()
                                                                        , 'doc_sog':String
                                                                        , 'date_doc_sog':DateTime()
                                                                        , 'time_since_last_status':Integer
                                                                        , 'sum_with_out_vat':Numeric
                                                                        , 'supplier_uuid': UUID(as_uuid=True)
                                                                        , 'storehouse': String
                                                                        , 'date_osnt': DateTime()},
                                                  'name_csv':'{{ dag.dag_id }}'},
                                       dag=dag)


_tg_df_empty = TelegramOperator(
                            task_id='tg_df_empty',
                            telegram_conn_id='telegram_conn_id',
                            chat_id='*****',
                            text="""DAG '{{ dag.dag_id }}' вернул пустой датафрейм. Автор дага: {{dag.owner}}, дата запуска: {{params.period[0]}}.""",
                            dag=dag,
                            params={'period': time_tg()}
                                )


_tg_failed = TelegramOperator(
                            task_id='send_message_telegram_failed',
                            telegram_conn_id='telegram_conn_id',
                            chat_id='*****',
                            text= """Даг '{{ dag.dag_id }}' упал или выполнился с ошибкой. Автор дага: {{dag.owner}}.""",
                            dag=dag,
                            trigger_rule=TriggerRule.ONE_FAILED
                        )

_query_generate>> _extract_data >> _select_next_task>> [_transform_data,_tg_df_empty] 
_transform_data >> _load_data >>_tg_failed
