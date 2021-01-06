from __future__ import print_function

import time
from builtins import range
from pprint import pprint

from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import PythonVirtualenvOperator
from airflow.operators.bash_operator import BashOperator


args = {
    'owner': 'Airflow',
    'start_date': days_ago(2), # don't know what this is doing
}

dag = DAG(
    dag_id='igloo_weather_historical_staging_only',
    default_args=args,
    schedule_interval=None,
    tags=['cdw'],
    catchup=False,
)

start_weather_staging_jobs = BashOperator(
    task_id='start_weather_staging_jobs',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_WeatherData && python start_weather_staging_jobs.py',
    dag=dag,
)


start_weather_staging_jobs
