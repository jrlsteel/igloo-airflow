from __future__ import print_function

import sys

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


from cdw.common.slack_utils import alert_slack

args = {
    "owner": "Airflow",
    "start_date": days_ago(2),  # don't know what this is doing
    "on_failure_callback": alert_slack,
}

dag = DAG(
    dag_id="ensek_nrl_mirror_only",
    default_args=args,
    schedule_interval=None,
    tags=["cdw"],
    catchup=False,
    max_active_runs=1,
)

mirror_nrl = BashOperator(
    task_id="mirror_nrl",
    bash_command="cd /opt/airflow/cdw/process_NRL && python start_nrl_mirror_only_jobs.py",
    dag=dag,
)


mirror_nrl
