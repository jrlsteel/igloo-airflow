import sys
sys.path.append("/opt/airflow/enzek-meterpoint-readings")

import datetime
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from process_smart.d0380_sftp_to_s3 import copy_all_from_d0380
from common.slack_utils import alert_slack
import sentry_sdk

args = {
    "owner": "Airflow",
    "start_date": days_ago(2),
    "on_failure_callback": alert_slack
}


dag = DAG(
    dag_id="igloo_smart_d0380",
    default_args=args,
    schedule_interval=None,
    tags=["cdw"],
    catchup=False,
)

def copy_all_from_d0380_wrapper():
    """
    :param: execution_date a string in the form 'YYYY-MM-DD'
    """
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
