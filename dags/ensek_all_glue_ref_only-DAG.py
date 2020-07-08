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
    dag_id='ensek_all_glue_ref_only',
    default_args=args,
    schedule_interval=None,
    tags=['cdw']
)

start_ensek_pa_ref_jobs = BashOperator(
    task_id='start_ensek_pa_ref_jobs',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_Ensek && python start_ensek_pa_ref_jobs.py',
    dag=dag,
)


start_ensek_non_pa_ref_jobs = BashOperator(
    task_id='start_ensek_non_pa_ref_jobs',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_Ensek && python start_ensek_non_pa_ref_jobs.py',
    dag=dag,
)



#process_customerdb >> process_ensek_meterpoints_no_history >> process_ensek_registration_meterpoint_status >> process_ensek_tariffs_history >> process_ensek_account_settings >> process_ensek_transactions >> start_ensek_pa_staging_jobs >> start_ensek_pa_ref_jobs >> start_ensek_non_pa_staging_jobs >> start_ensek_non_pa_ref_jobs

start_ensek_pa_ref_jobs >> start_ensek_non_pa_ref_jobs
