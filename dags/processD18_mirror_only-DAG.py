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
    dag_id='igloo_d18_mirror_only',
    default_args=args,
    schedule_interval=None,
    tags=['cdw'],
    catchup=False,
    max_active_runs=1,
)

start_d18_mirror_only_jobs = BashOperator(
    task_id='start_processD18_mirror_jobs.py',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_D18 && python start_processD18_mirror_jobs.py',
    dag=dag,
)

start_d18_mirror_only_jobs