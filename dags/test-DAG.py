import sys

sys.path.append("/opt/airflow/enzek-meterpoint-readings")

import datetime
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from process_smart.d0379 import generate_d0379, copy_d0379_to_sftp
import sentry_sdk
from common.slack_utils import alert_slack

args = {"owner": "Airflow", "start_date": days_ago(2), "on_failure_callback": alert_slack}

dag = DAG(
    dag_id="igloo_test_dag",
    default_args=args,
    schedule_interval=None,
    tags=["cdw"],
    catchup=False,
    description="The igloo-test-dag is intended to provide a place to experiment",
)


def test_task_1_fn(execution_date):
    """
    :param: execution_date a string in the form 'YYYY-MM-DD'
    """
    try:
        pass
    except Exception as e:
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush(5)
        raise e


def test_task_2_fn(execution_date):
    """
    :param: execution_date a string in the form 'YYYY-MM-DD'
    """
    try:
        raise Exception("An error occurred in test_task_2")
    except Exception as e:
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush(5)
        raise e


test_task_1 = PythonOperator(
    task_id="test_task_1",
    python_callable=test_task_1_fn,
    op_args=["{{ ds }}"],
    dag=dag,
    description_md="""# Purpose

This step extracts data from the Ensek API

# Suggested action on failure

No action

# Justification

This step has a very long run time, and the impact of failure is low. See [here](https://example.org) for more details.
""",
)

test_task_2 = PythonOperator(
    task_id="test_task_2",
    python_callable=test_task_2_fn,
    op_args=["{{ ds }}"],
    dag=dag,
    description_md="""# Purpose

This step extracts data from the Ensek API

# Suggested action on failure

If the failure is detected within 3 hours, it should be rerun.

# Justification

As per [this documentation](https://example.org), if this step can complete prior to the calculated steps that start at 06:00 then rerunning is beneficial.

""",
)

test_task_1 >> test_task_2
