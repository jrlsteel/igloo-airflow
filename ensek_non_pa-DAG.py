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
    dag_id='ensek_pa',
    default_args=args,
    schedule_interval=None,
    tags=['cdw']
)

process_ensek_registration_meterpoint_status = BashOperator(
    task_id='process_ensek_registration_meterpoint_status.py',
    bash_command='cd /usr/local/airflow/dags/enzek-meterpoint-readings/process_Ensek && ../.venv/bin/python processEnsekStatus/process_ensek_registration_meterpoint_status.py',
    dag=dag,
)

process_ensek_internal_estimates = BashOperator(
    task_id='process_ensek_internal_estimates',
    bash_command='cd /usr/local/airflow/dags/enzek-meterpoint-readings/process_Ensek && ../.venv/bin/python processEnsekEstimates/process_ensek_internal_estimates.py',
    dag=dag,
)

process_ensek_tariffs_history = BashOperator(
    task_id='process_ensek_tariffs_history',
    bash_command='cd /usr/local/airflow/dags/enzek-meterpoint-readings/process_Ensek && ../.venv/bin/python processEnsekTariffs/process_ensek_tariffs_history.py',
    dag=dag,
)

process_ensek_account_settings = BashOperator(
    task_id='process_ensek_account_settings',
    bash_command='cd /usr/local/airflow/dags/enzek-meterpoint-readings/process_Ensek && ../.venv/bin/python processEnsekAccountSettings/process_ensek_account_settings.py',
    dag=dag,
)

process_ensek_transactions = BashOperator(
    task_id='process_ensek_transactions',
    bash_command='cd /usr/local/airflow/dags/enzek-meterpoint-readings/process_Ensek && ../.venv/bin/python processEnsekTransactions/process_ensek_transactions.py',
    dag=dag,
)

process_ensek_registration_meterpoint_status >> process_ensek_internal_estimates >> process_ensek_tariffs_history >> process_ensek_account_settings >> process_ensek_transactions
