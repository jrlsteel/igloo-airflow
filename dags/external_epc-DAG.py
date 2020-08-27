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
    dag_id='igloo_epc',
    default_args=args,
    # schedule_interval='45 09 * * *',
    schedule_interval=None,
    tags=['cdw']
)


start_epc_full_processing_jobs = BashOperator(
    task_id='start_epc_full_processing_jobs',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_EPC && python start_epc_full_processing_jobs.py',
    dag=dag,
)

start_epc_full_staging_jobs = BashOperator(
    task_id='start_epc_full_staging_jobs',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_EPC && python start_epc_full_staging_jobs.py',
    dag=dag,
)

start_epc_full_ref_jobs = BashOperator(
    task_id='start_epc_full_ref_jobs',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_EPC && python start_epc_full_ref_jobs.py',
    dag=dag,
)

start_epc_full_processing_jobs >> start_epc_full_staging_jobs >> start_epc_full_ref_jobs
