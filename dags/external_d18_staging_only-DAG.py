from __future__ import print_function

import time
import sys
from builtins import range
from pprint import pprint

from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import PythonVirtualenvOperator
from airflow.operators.bash_operator import BashOperator


from cdw.common.slack_utils import alert_slack

args = {
    "owner": "Airflow",
    "start_date": days_ago(2),  # don't know what this is doing
    "on_failure_callback": alert_slack,
}

dag = DAG(
    dag_id="igloo_d18_staging_only",
    default_args=args,
    schedule_interval=None,
    tags=["cdw"],
    catchup=False,
    max_active_runs=1,
)

start_d18_staging_jobs = BashOperator(
    task_id="start_d18_staging_jobs",
    bash_command="cd /opt/airflow/cdw/process_D18 && python start_d18_staging_jobs.py",
    dag=dag,
)


start_d18_staging_jobs
