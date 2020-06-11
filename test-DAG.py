from __future__ import print_function

import time
from builtins import range
from pprint import pprint

from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2), # don't know what this is doing
}

dag = DAG(
    dag_id='start_ensek_api_pa_jobs',
    default_args=args,
    schedule_interval=None,
    tags=['start_ensek_api_pa_jobs']
)

# submit_customerDB_Gluejob = PythonOperator(
#     task_id='submit_customerDB_Gluejob',
#     provide_context=True,
#     python_callable=StartEnsekPAJobs.submit_customerDB_Gluejob,
#     dag=dag,
# )

process_ensek_meterpoints_no_history = BashOperator(
    task_id='process_ensek_meterpoints_no_history',
    bash_command='./process_Ensek/processEnsekMeterpoints/process_ensek_meterpoints_no_history.py',
    dag=dag,
)

# customerDB_Gluejob runs before submit_all_ensek_pa_scripts
# submit_customerDB_Gluejob >> submit_all_ensek_pa_scripts
