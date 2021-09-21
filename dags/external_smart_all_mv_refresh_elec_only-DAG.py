from __future__ import print_function

import time
from builtins import range
from pprint import pprint

from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import sys

from cdw.process_smart.start_smart_refresh_mv_hh import refresh_mv_hh_elec_reads

from cdw.common.slack_utils import alert_slack

args = {
    "owner": "Airflow",
    "start_date": days_ago(2),  # don't know what this is doing
    "on_failure_callback": alert_slack,
}

dag = DAG(
    dag_id="igloo_smart_refresh_mv_hh_elec_reads",
    default_args=args,
    schedule_interval=None,
    tags=["cdw"],
    catchup=False,
    max_active_runs=1,
)

smart_all_refresh_mv_hh_elec_reads_jobs_task = PythonOperator(
    dag=dag,
    task_id="smart_all_refresh_mv_hh_elec_reads_jobs_task",
    python_callable=refresh_mv_hh_elec_reads,
)


smart_all_refresh_mv_hh_elec_reads_jobs_task
