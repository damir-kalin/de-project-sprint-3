import time
import requests
import json
import pandas as pd

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook
from airflow.operators.dummy import DummyOperator

http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host

postgres_conn_id = 'postgresql_de'

nickname = "damir-kalinin7"
cohort = "22"

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}

def generate_report(ti):
    print('Making request generate_report')

    response = requests.post(f'{base_url}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    print(f'Response is {response.content}')


def get_report(ti):
    print('Making request get_report')
    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    for i in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()
        print(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError()

    ti.xcom_push(key='report_id', value=report_id)
    print(f'Report_id={report_id}')


def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    report_id = ti.xcom_pull(key='report_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{report_id}/{filename}'
    print(s3_filename)
    local_filename = date.replace('-', '') + '_' + filename
    print(local_filename)
    response = requests.get(s3_filename)
    response.raise_for_status()
    open(f"{local_filename}", "wb").write(response.content)
    print(response.content)

    if pg_table == 'user_order_log':
        df = pd.read_csv(local_filename)
        df=df.drop('id', axis=1)
        df=df.drop_duplicates(subset=['uniq_id'])
        if 'status' not in df.columns:
            df['status'] = 'shipped'
    if pg_table == 'user_activity_log':
        df = pd.read_csv(local_filename)
        df=df.drop('id', axis=1)
        df=df.drop_duplicates(subset=['uniq_id'])
    if pg_table == 'price_log':
        df = pd.read_csv(local_filename, header=None, names=['name', 'price'])
    if pg_table == 'customer_research':
        df = pd.read_csv(local_filename)

    print(df.columns)

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    row_count = df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)
    print(f'{row_count} rows was inserted')


args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

business_dt = '{{ ds }}'

with DAG(
        'sales_mart',
        default_args=args,
        description='Provide default dag for sprint3',
        catchup=True,
        start_date=datetime.today(),
        max_active_runs = 1
) as dag:

    staging_schema_create = PostgresOperator(
        task_id='staging_schema_create',
        postgres_conn_id=postgres_conn_id,
        sql="sql/staging_schema_create.sql")

    truncate_customer_research = PostgresOperator(
        task_id='truncate_customer_research',
        postgres_conn_id=postgres_conn_id,
        sql="sql/truncate_customer_research.sql")

    truncate_user_activity_log = PostgresOperator(
        task_id='truncate_user_activity_log',
        postgres_conn_id=postgres_conn_id,
        sql="sql/truncate_user_activity_log.sql")  
    
    truncate_user_order_log = PostgresOperator(
        task_id='truncate_user_order_log',
        postgres_conn_id=postgres_conn_id,
        sql="sql/truncate_user_order_log.sql")

    truncate_price_log = PostgresOperator(
        task_id='truncate_price_log',
        postgres_conn_id=postgres_conn_id,
        sql="sql/truncate_price_log.sql")

    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report)

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report)

    upload_user_order = PythonOperator(
        task_id='upload_user_order',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_order_log.csv',
                   'pg_table': 'user_order_log',
                   'pg_schema': 'staging'})
    
    upload_user_activity = PythonOperator(
        task_id='upload_user_activity',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_activity_log.csv',
                   'pg_table': 'user_activity_log',
                   'pg_schema': 'staging'})

    upload_customer_research = PythonOperator(
        task_id='upload_customer_research',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'customer_research.csv',
                   'pg_table': 'customer_research',
                   'pg_schema': 'staging'})
    
    upload_price = PythonOperator(
        task_id='upload_price',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'price_log.csv',
                   'pg_table': 'price_log',
                   'pg_schema': 'staging'})
    
    update_d_item_table = PostgresOperator(
        task_id='update_d_item',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_item.sql")

    update_d_city_table = PostgresOperator(
        task_id='update_d_city',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_city.sql")

    update_d_customer_table = PostgresOperator(
        task_id='update_d_customer',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_customer.sql")

    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales.sql",
        parameters={"date": {business_dt}}
    )

staging_schema_create >> [truncate_customer_research, truncate_user_activity_log, truncate_user_order_log, truncate_price_log] >> generate_report 
generate_report >> get_report >> [upload_user_order, upload_user_activity, upload_customer_research, upload_price] 
upload_user_order >>[update_d_item_table, update_d_city_table] 
update_d_city_table >> update_d_customer_table
[update_d_item_table, update_d_customer_table] >> update_f_sales

