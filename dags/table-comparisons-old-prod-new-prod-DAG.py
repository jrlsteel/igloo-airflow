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
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='igloo_table_comparisons_old_prod_new_prod',
    default_args=args,
    schedule_interval=None,
    tags=['cdw']
)

ref_meterpoints_old_preprod_new_preprod = BashOperator(
    task_id='compare_tables',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_table_comparisons && python compare_tables.py --table-comparison-config ref_meterpoints_old_preprod_new_preprod --output-to-s3',
    dag=dag,
)

ref_meterpoints_old_preprod_new_preprod
