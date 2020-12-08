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
    dag_id='ensek_occupier_accounts',
    default_args=args,
    schedule_interval=None,
    tags=['cdw']
)

process_ensek_occupier_accounts = BashOperator(
    task_id='process_ensek_occupier_accounts',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/processEnsekOccupierAccounts && python process_ensek_occupier_accounts.py',
    dag=dag,
)

#Not required as very small file
# start_ensek_occupier_accounts_staging_jobs = BashOperator(
#     task_id='start_ensek_occupier_accounts_staging_jobs',
#     bash_command='cd /opt/airflow/enzek-meterpoint-readings/processEnsekOccupierAccounts && python start_ensek_occupier_accounts_staging_jobs.py',
#     dag=dag,
# )

start_ensek_occupier_accounts_ref_jobs = BashOperator(
    task_id='start_ensek_occupier_accounts_ref_jobs',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/processEnsekOccupierAccounts && python start_ensek_occupier_accounts_ref_jobs.py',
    dag=dag,
)

start_ensek_api_mirror_only_jobs = BashOperator(
    task_id='start_ensek_occupier_accounts_mirror_jobs.py',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_Ensek/processEnsekOccupierAccounts && python start_ensek_occupier_accounts_mirror_jobs.py',
    dag=dag,
)

start_ensek_api_mirror_only_jobs >> start_ensek_occupier_accounts_ref_jobs
