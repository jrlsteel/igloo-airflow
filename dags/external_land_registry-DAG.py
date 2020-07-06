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
    dag_id='igloo_land_registry',
    default_args=args,
    schedule_interval=None,
    tags=['cdw']
)

process_land_registry = BashOperator(
    task_id='process_land_registry',
    bash_command='cd /opt/code/enzek-meterpoint-readings/process_LandRegistry && python process_land_registry.py',
    dag=dag,
)

start_land_registry_staging_jobs = BashOperator(
    task_id='start_land_registry_staging_jobs',
    bash_command='cd /opt/code/enzek-meterpoint-readings/process_LandRegistry && python start_land_registry_staging_jobs.py',
    dag=dag,
)

start_land_registry_ref_jobs = BashOperator(
    task_id='start_land_registry_ref_jobs',
    bash_command='cd /opt/code/enzek-meterpoint-readings/process_LandRegistry && python process_land_registry.py',
    dag=dag,
)

process_land_registry >> start_land_registry_staging_jobs >> start_land_registry_ref_jobs
