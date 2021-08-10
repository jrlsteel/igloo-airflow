import sys

sys.path.append("/opt/airflow/enzek-meterpoint-readings")

from airflow.operators.dagrun_operator import TriggerDagRunOperator
import datetime
import time
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from common.slack_utils import post_slack_sla_alert
import json

# args = {"owner": "Airflow", "start_date": days_ago(2), "sla": datetime.datetime.today().replace(hour=8, minute=0, second=0, microsecond=0), "on_failure_callback": alert_slack}
args = {
    "owner": "Airflow",
    "start_date": days_ago(2),
    "sla": datetime.timedelta(seconds=10),
}


def do_some_stuff():
    time.sleep(30)
    print("Main dag finished")


dag = DAG(
    dag_id="igloo_test_sla_alert_dag",
    default_args=args,
    sla_miss_callback=post_slack_sla_alert,
    schedule_interval="* * * * *",
    tags=["cdw"],
    catchup=False,
    description="This DAG is intended to test SLA alerting for DAG overruns",
)

test_task_1 = PythonOperator(task_id="do_some_stuff", python_callable=do_some_stuff, dag=dag)
