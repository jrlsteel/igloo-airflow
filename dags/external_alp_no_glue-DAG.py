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
    dag_id='igloo_alp_no_glue',
    default_args=args,
    schedule_interval=None,
    tags=['cdw']
)

processALP_CV = BashOperator(
    task_id='processALP_CV',
    bash_command='cd /opt/code/enzek-meterpoint-readings/process_ALP && ../.venv/bin/python processALP_CV.py',
    dag=dag,
)

processALP_WCF = BashOperator(
    task_id='processALP_WCF',
    bash_command='cd /opt/code/enzek-meterpoint-readings/process_ALP && ../.venv/bin/python processALP_WCF.py',
    dag=dag,
)


processALP_CV >> processALP_WCF



