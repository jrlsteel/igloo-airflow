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

sys.path.append("/opt/airflow/enzek-meterpoint-readings")

from conf import config
from common import utils as util
from process_go_cardless.go_cardless_report_download import GoCardlessReport
import sentry_sdk
from common.slack_utils import alert_slack

args = {
    "owner": "Airflow",
    "start_date": days_ago(2),  #  don't know what this is doing
    "on_failure_callback": alert_slack,
}

dag = DAG(
    dag_id="go_cardless_report",
    default_args=args,
    schedule_interval="30 09 * * *",
    tags=["cdw"],
    catchup=False,
    max_active_runs=1,
)


def download_go_cardless_report_wrapper(execution_date):
    """
    :param: execution_date a string in the form 'YYYY-MM-DD'
    """
    try:
        print("download_go_cardless_report  execution_date={}".format(execution_date))
        directory = util.get_dir()
        bucket_name = directory["s3_finance_bucket"]
        instance = GoCardlessReport(config, bucket_name)
        instance.process()
    except Exception as e:
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush(5)
        raise e


download_go_cardless_report_task = PythonOperator(
    task_id="download_go_cardless_report",
    python_callable=download_go_cardless_report_wrapper,
    op_args=["{{ ds }}" if util.get_env() == "newprod" else "{{ prev_ds }}"],
    dag=dag,
)
