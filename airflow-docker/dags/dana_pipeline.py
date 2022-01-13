from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from os.path import dirname, abspath
from airflow.models import Variable
import json
import pandas as pd
import sys, os

default_args = {
    'owner': 'airflow',
    'depend_on_past': False,
    'start_date': datetime(2021, 1, 13),
    'email': ['fjrama.dhn@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'concurrency': 2,
    'retries': 1,
    'retry_delay': timedelta(seconds=60),
    #'on_failure_callback': slack_hook
    # 'on_retry_callback': slack_hook
}

dag = DAG('dana_pipeline', default_args=default_args, catchup=False, schedule_interval='0 */6 * * *')
parent = dirname(dirname(dirname(abspath(__file__))))
start_task = DummyOperator(task_id='start_task', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)

#get variable
staging_tables = Variable.get("staging_tables")
staging_ingest_type = Variable.get("staging_ingest_type")
dwh_ingest_type = Variable.get("dwh_ingest_type")

staging_layer = list(staging_tables.split(","))
list_staging = []

#process staging
for table in staging_layer:
    file_path = Variable.get(f"file_path_{table}")
    ingest_staging = BashOperator(task_id=f'ingest_staging_{table}',
                        bash_command=f'python3 {parent}/script/main_staging.py {file_path} {table} {staging_ingest_type}',
                        dag=dag)
    list_staging.append(ingest_staging)

#process dwh
ingest_dwh_dim_weather = BashOperator(task_id=f'ingest_dwh_dim_weather',
                        bash_command=f'python3 {parent}/script/main_dwh.py dim_weather {dwh_ingest_type}',
                        dag=dag)
                    
ingest_dwh_fact_review = BashOperator(task_id=f'ingest_dwh_fact_review',
                        bash_command=f'python3 {parent}/script/main_dwh.py fact_review {dwh_ingest_type}',
                        dag=dag)

#create workflow task               
start_task >> list_staging >> ingest_dwh_dim_weather >> ingest_dwh_fact_review >> end_task