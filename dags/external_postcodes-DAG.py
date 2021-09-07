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
    dag_id="postcodes_etl",
    default_args=args,
    # The Postcode data is updated approximately once per quarter, although an exact
    # schedule for this is not available. We will just download it once per month.
    # The exact schedule is somewhat arbitrary.
    schedule_interval="18 12 15 * *",
    tags=["cdw"],
    catchup=False,
    max_active_runs=1,
)

postcodes_etl = BashOperator(
    task_id="postcodes_etl",
    bash_command="cd /opt/airflow/cdw/process_postcodes && python postcodes_etl.py",
    dag=dag,
)

postcodes_etl
