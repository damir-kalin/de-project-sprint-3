import time
import requests
import json
import pandas as pd
import logging

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook
from airflow.operators.dummy import DummyOperator

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


def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    report_id = ti.xcom_pull(key='report_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT}/{NICKNAME}/project/{report_id}/{filename}'
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
        df = df.drop_duplicates(subset=['uniq_id'])
        logging.info('Duplicates removed')
        if 'status' not in df.columns:
            df['status'] = 'shipped'
            logging.info('Added status column')
    if pg_table == 'user_activity_log':
        df = pd.read_csv(local_filename, index_col=0)
        logging.info('File reading completed')
        df = df.drop_duplicates(subset=['uniq_id'])
        logging.info('Duplicates removed')
    if pg_table == 'price_log':
        df = pd.read_csv(local_filename, header=None, names=['name', 'price'])
        logging.info('File reading completed')
    if pg_table == 'customer_research':
        df = pd.read_csv(local_filename)
        logging.info('File reading completed')
    logging.info(f"Columns in table - {df.columns}")

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    row_count = df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)
    logging.info(f'{row_count} rows was inserted')


args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2
}

business_dt = '{{ ds }}'

dag = DAG(
        'report',
        default_args=args,
        description='Provide default dag for sprint3',
        catchup=True,
        start_date=datetime.now(),
        max_active_runs = 1) 

staging_schema_create = PostgresOperator(
    task_id='staging_schema_create',
    postgres_conn_id=postgres_conn_id,
    sql="sql/staging_schema_create.sql",
    dag=dag)

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
    dag=dag)

get_report = PythonOperator(
    task_id='get_report',
    python_callable=get_report)

upload_user_order = PythonOperator(
    task_id='upload_user_order',
    python_callable=upload_data_to_staging,
    op_kwargs={
        'date': business_dt,
        'filename': 'user_order_log.csv',
        'pg_table': 'user_order_log',
        'pg_schema': 'staging'
    },
    dag=dag
    )

upload_customer_research = PythonOperator(
    task_id='upload_customer_research',
    python_callable=upload_data_to_staging,
    op_kwargs={
        'date': business_dt,
        'filename': 'customer_research.csv',
        'pg_table': 'customer_research',
        'pg_schema': 'staging'
    },
    dag=dag
    )

upload_user_activity_log = PythonOperator(
    task_id='upload_user_activity_log',
    python_callable=upload_data_to_staging,
    op_kwargs={
        'date': business_dt,
        'filename': 'user_activity_log.csv',
        'pg_table': 'user_activity_log',
        'pg_schema': 'staging'
    },
    dag=dag
    )
upload_price_log = PythonOperator(
    task_id='upload_price_log',
    python_callable=upload_data_to_staging,
    op_kwargs={
        'date': business_dt,
        'filename': 'price_log.csv',
        'pg_table': 'price_log',
        'pg_schema': 'staging'
    },
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
    sql="sql/mart.f_sales.sql",
    parameters={"date": {business_dt}},
    dag=dag
)

staging_schema_create >> l_trunc_table_tasks >> generate_report >> get_report
get_report >>  [upload_user_order,  upload_customer_research, upload_user_activity_log, upload_price_log]
upload_user_order >> l_update_tasks >> update_f_sales

