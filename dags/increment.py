import time
import requests
import json
import pandas as pd
import logging
import os

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

task_logger = logging.getLogger("airflow.task")


http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host

postgres_conn_id = 'postgresql_de'

NICKNAME = "damir-kalinin7"
COHORT = "22"

headers = {
    'X-Nickname': NICKNAME,
    'X-Cohort': COHORT,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}

def generate_report(ti):
    logging.info('Making request generate_report')
    
    response = requests.post(f'{base_url}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    logging.info(f'Response is {response.content}')


def get_report(ti):
    logging.info('Making request get_report')
    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    for i in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()
        logging.info(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        logging.error('Request failed')
        raise TimeoutError()

    ti.xcom_push(key='report_id', value=report_id)
    logging.info(f'Report_id={report_id}')

def get_increment(date, ti):
    #logging.info(f'-------------------- {datetime.strptime(date, "%Y-%m-%d").date()}')
    logging.info('Making request get_increment')
    report_id = ti.xcom_pull(key='report_id')
    response = requests.get(
        f'{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=headers)
    response.raise_for_status()
    logging.info(f'Response is {response.content}')

    increment_id = json.loads(response.content)['data']['increment_id']
    if not increment_id:
        logging.error('Increment not received')
        raise ValueError(f'Increment is empty. Most probably due to error in API call.')
    
    ti.xcom_push(key='increment_id', value=increment_id)
    logging.info(f'increment_id={increment_id}')

def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    increment_id = ti.xcom_pull(key='increment_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT}/{NICKNAME}/project/{increment_id}/{filename}'
    logging.info(s3_filename)
    local_filename = date.replace('-', '') + '_' + filename
    logging.info(local_filename)
    response = requests.get(s3_filename)
    response.raise_for_status()
    open(f"{local_filename}", "wb").write(response.content)
    logging.info(response.content)

    if pg_table == 'user_order_log':
        df = pd.read_csv(local_filename, index_col=0)
        logging.info('File reading completed')
        df=df.drop_duplicates(subset=['uniq_id'])
        logging.info('Duplicates removed')
        if 'status' not in df.columns:
            df['status'] = 'shipped'
            logging.info('Added status column')
    if pg_table == 'user_activity_log':
        df = pd.read_csv(local_filename, index_col=0)
        logging.info('File reading completed')
        df=df.drop_duplicates(subset=['uniq_id'])
        logging.info('Duplicates removed')
    if pg_table == 'price_log':
        df = pd.read_csv(local_filename, header=None, names=['name', 'price'])
        logging.info('File reading completed')
    if pg_table == 'customer_research':
        df = pd.read_csv(local_filename)
        logging.info('File reading completed')

    logging.info(f'Columns in table - {df.columns}')

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    row_count = df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)
    logging.info(f'{row_count} rows was inserted')

def is_monday(date, ti):
    dt = datetime.strptime(date, '%Y-%m-%d').weekday()
    logging.info(f'Weekday - {dt}')
    return dt == 1



args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2
}

business_dt = '{{ ds }}'

dag = DAG(
    'increment',
    default_args=args,
    description='Provide default dag for sprint3',
    catchup=True,
    start_date=datetime.today() - timedelta(days=7),
    end_date=datetime.today() - timedelta(days=1),
    max_active_runs = 1
) 

staging_schema_create = PostgresOperator(
    task_id='staging_schema_create',
    postgres_conn_id=postgres_conn_id,
    sql="sql/staging_schema_create.sql",
    dag = dag
)

l_trunc_table_tasks = list()
for i in ['truncate_customer_research', 'truncate_user_activity_log', 'truncate_user_order_log', 'truncate_price_log']:
    l_trunc_table_tasks.append(PostgresOperator(
        task_id=i,
        postgres_conn_id=postgres_conn_id,
        sql=f'sql/{i}.sql',
        dag=dag
        )
)

generate_report = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag
)

get_report = PythonOperator(
    task_id='get_report',
    python_callable=get_report,
    dag=dag
)

get_increment = PythonOperator(
    task_id='get_increment',
    python_callable=get_increment,
    op_kwargs={'date': business_dt},
    dag=dag
)
    
upload_user_order_inc = PythonOperator(
    task_id='upload_user_order_inc',
    python_callable=upload_data_to_staging,
    op_kwargs={'date': business_dt,
                'filename': 'user_order_log_inc.csv',
                'pg_table': 'user_order_log',
                'pg_schema': 'staging'},
    dag=dag
)

upload_customer_research_inc = PythonOperator(
    task_id='upload_customer_research_inc',
    python_callable=upload_data_to_staging,
    op_kwargs={'date': business_dt,
                'filename': 'customer_research_inc.csv',
                'pg_table': 'customer_research',
                'pg_schema': 'staging'},
    dag=dag
)

upload_user_activity_log_inc = PythonOperator(
    task_id='upload_user_activity_log_inc',
    python_callable=upload_data_to_staging,
    op_kwargs={'date': business_dt,
                'filename': 'user_activity_log_inc.csv',
                'pg_table': 'user_activity_log',
                'pg_schema': 'staging'},
    dag=dag
)

l_update_tasks = list()
for i in ['d_item', 'd_city', 'd_customer']:
    l_update_tasks.append(PostgresOperator(
        task_id=f'update_{i}',
        postgres_conn_id=postgres_conn_id,
        sql=f'sql/mart.{i}.sql',
        dag=dag
        )
)

update_f_sales = PostgresOperator(
    task_id='update_f_sales',
    postgres_conn_id=postgres_conn_id,
    sql="sql/mart.f_sales_inc.sql",
    parameters={'date': {business_dt}},
    dag=dag
)

is_monday = ShortCircuitOperator(
    task_id='is_monday',
    python_callable=is_monday,
    op_kwargs = {"date": business_dt}
)

update_f_customer_retention = PostgresOperator(
    task_id='update_f_customer_retention',
    postgres_conn_id=postgres_conn_id,
    sql="sql/mart.f_customer_retention_inc.sql",
    parameters={"date": {business_dt}},
    dag=dag
)

 
staging_schema_create >> l_trunc_table_tasks >> generate_report 
generate_report >> get_report >> get_increment
get_increment >> [upload_user_order_inc, upload_user_activity_log_inc, upload_customer_research_inc] 
upload_user_order_inc >> l_update_tasks >> update_f_sales >> is_monday >> update_f_customer_retention, 
