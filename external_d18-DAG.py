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
    dag_id='igloo_d18',
    default_args=args,
    schedule_interval=None,
    tags=['cdw']
)

start_d18_download_jobs = BashOperator(
    task_id='start_d18_download_jobs',
    bash_command='cd /usr/local/airflow/dags/enzek-meterpoint-readings/process_D18 && ../.venv/bin/python start_d18_download_jobs.py',
    dag=dag,
)


start_d18_processing_jobs = BashOperator(
    task_id='start_d18_processing_jobs',
    bash_command='cd /usr/local/airflow/dags/enzek-meterpoint-readings/process_D18 && ../.venv/bin/python start_d18_processing_jobs.py',
    dag=dag,
)

start_d18_staging_jobs = BashOperator(
    task_id='start_d18_staging_jobs',
    bash_command='cd /usr/local/airflow/dags/enzek-meterpoint-readings/process_D18 && ../.venv/bin/python start_d18_staging_jobs.py',
    dag=dag,
)

start_d18_ref_jobs = BashOperator(
    task_id='start_d18_ref_jobs',
    bash_command='cd /usr/local/airflow/dags/enzek-meterpoint-readings/process_D18 && ../.venv/bin/python start_d18_ref_jobs.py',
    dag=dag,
)

start_d18_download_jobs >> start_d18_processing_jobs >> start_d18_staging_jobs  >> start_d18_ref_jobs
