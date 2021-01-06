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
    dag_id='go_cardless',
    default_args=args,
    schedule_interval='30 05 * * *',
    tags=['cdw'],
    catchup=False,
)

start_go_cardless_api_extracts = BashOperator(
    task_id='start_go_cardless_api_extracts',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_go_cardless && python start_go_cardless_api_extracts.py',
    dag=dag,
)

start_go_cardless_api_extracts
