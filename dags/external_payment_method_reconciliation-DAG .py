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
from process_payment_method_reconciliation.validate_payment_methods import PaymentMethodValidator
import sentry_sdk
from common.slack_utils import alert_slack

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
    'on_failure_callback': alert_slack,
}

dag = DAG(
    dag_id='payment_method_reconciliation',
    default_args=args,
    schedule_interval=None,
    tags=['cdw'],
    catchup=False,
    max_active_runs=1,
)

def validate_payment_methods_wrapper(execution_date):
    """
    :param: execution_date a string in the form 'YYYY-MM-DD'
    """
    try:
        print("validate_payment_methods  execution_date={}".format(execution_date))
        directory = util.get_dir()
        finance_bucket = directory["s3_finance_bucket"]
        api_key = directory['apis']['token']
        ensek_config = {
            "ensek_api": {
                "base_url": "https://api.igloo.ignition.ensek.co.uk",
                "api_key": api_key
            }
        }

        instance = PaymentMethodValidator(
            config=config,
            ensek_config=ensek_config,
            bucket_name=finance_bucket,
            # sample_size=1000,
        )
        instance.process()
    except Exception as e:
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush(5)
        raise e

validate_payment_methods_task = PythonOperator(
    task_id="validate_payment_methods",
    python_callable=validate_payment_methods_wrapper,
    op_args=["{{ ds }}"],
    dag=dag,
)

validate_payment_methods_task
