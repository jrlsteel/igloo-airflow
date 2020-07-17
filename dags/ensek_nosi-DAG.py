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
    dag_id='ensek_nosi',
    default_args=args,
    schedule_interval='45 12 * * *',
    tags=['cdw']
)

mirror_nosi = BashOperator(
    task_id='mirror_nosi',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_Nosi && python start_ensek_readings_nosi_mirror_only_jobs.py',
    dag=dag,
)

start_ensek_readings_nosi_staging_jobs = BashOperator(
    task_id='start_ensek_readings_nosi_staging_jobs',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_Nosi && python start_ensek_readings_nosi_staging_jobs.py',
    dag=dag,
)

start_ensek_readings_nosi_ref_jobs = BashOperator(
    task_id='start_ensek_readings_nosi_ref_jobs',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_Nosi && python start_ensek_readings_nosi_ref_jobs.py',
    dag=dag,
)

mirror_nosi >> start_ensek_readings_nosi_staging_jobs >> start_ensek_readings_nosi_ref_jobs
