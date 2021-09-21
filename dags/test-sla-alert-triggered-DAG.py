import sys


from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import datetime
import time
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from cdw.common.slack_utils import post_slack_message
import json

# args = {"owner": "Airflow", "start_date": days_ago(2), "sla": datetime.datetime.today().replace(hour=8, minute=0, second=0, microsecond=0), "on_failure_callback": alert_slack}
args = {
    "owner": "Airflow",
    "start_date": days_ago(2),
    "sla": datetime.timedelta(minutes=3),
}


def send_sla_alert(dag, task_list, blocking_task_list, slas, blocking_tis):
    post_slack_message("[Triggered DAG] Successfully timed out after 3 minutes.", "test-alerts")


def do_some_other_stuff():
    time.sleep(100)
    print("Triggered dag finished")


sensor = ExternalTaskSensor(
    task_id="sensor_task",
    external_dag_id="igloo_test_sla_alert_dag",
    external_task_id="do_some_stuff",
    execution_date=datetime.datetime.today().strftime("%Y-%m-%d"),
    start_date=days_ago(2),
)

dag = DAG(
    dag_id="igloo_test_sla_alert_triggered_dag",
    default_args=args,
    sla_miss_callback=send_sla_alert,
    schedule_interval="55 * * * *",
    tags=["cdw"],
    catchup=False,
    description="This DAG is intended to test SLA alerting for DAG overruns",
)

test_task_1 = PythonOperator(task_id="do_some_other_stuff", python_callable=do_some_other_stuff, dag=dag)

sensor >> test_task_1
