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
    dag_id='ensek_pa_no_glue',
    default_args=args,
    schedule_interval=None,
    tags=['cdw']
)



process_ensek_meterpoints_no_history = BashOperator(
    task_id='process_ensek_meterpoints_no_history',
    bash_command='cd /opt/code/enzek-meterpoint-readings/process_Ensek && ../.venv/bin/python processEnsekMeterpoints/process_ensek_meterpoints_no_history.py',
    dag=dag,
)

process_ensek_internal_readings = BashOperator(
    task_id='process_ensek_internal_readings',
    bash_command='cd /opt/code/enzek-meterpoint-readings/process_Ensek && ../.venv/bin/python processEnsekReadings/process_ensek_internal_readings.py',
    dag=dag,
)

start_ensek_pa_staging_jobs = BashOperator(


process_ensek_meterpoints_no_history >> process_ensek_internal_readings 

#process_ensek_meterpoints_no_history >> process_ensek_internal_readings >> start_ensek_pa_staging_jobs >> start_ensek_pa_ref_jobs
