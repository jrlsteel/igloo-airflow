import sys
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator


from cdw.conf import config
from cdw.common import schedules
from cdw.process_smart.d0380_sftp_to_s3 import copy_all_from_d0380
from cdw.common.slack_utils import alert_slack
import sentry_sdk

dag_id = "igloo_smart_d0380"


def get_schedule():
    env = config.environment_config["environment"]
    return schedules.get_schedule(env, dag_id)


args = {
    "owner": "Airflow",
    "start_date": days_ago(2),
    "on_failure_callback": alert_slack,
}


dag = DAG(
    dag_id=dag_id,
    default_args=args,
    schedule_interval=get_schedule(),
    tags=["cdw"],
    catchup=False,
)


def copy_all_from_d0380_wrapper():
    try:
        copy_all_from_d0380()
    except Exception as e:
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush(5)
        raise e


copy_all_from_d0380_task = PythonOperator(
    task_id="copy_all_from_d0380",
    python_callable=copy_all_from_d0380_wrapper,
    dag=dag,
)

copy_all_from_d0380_task
