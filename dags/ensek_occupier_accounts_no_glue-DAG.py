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
    dag_id='ensek_occupier_accounts_no_glue',
    default_args=args,
    schedule_interval=None,
    tags=['cdw']
)

process_ensek_occupier_accounts = BashOperator(
    task_id='process_ensek_occupier_accounts',
    bash_command='cd /opt/code/enzek-meterpoint-readings/process_Ensek/processEnsekOccupierAccounts && python process_ensek_occupier_accounts.py',
    dag=dag,
)



process_ensek_occupier_accounts
