import sys


import datetime
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from cdw.process_smart.d0379 import generate_d0379, copy_d0379_to_sftp
import sentry_sdk
from cdw.common.slack_utils import alert_slack
import requests
from requests_aws4auth import AWS4Auth
import os

args = {"owner": "Airflow", "start_date": days_ago(2), "on_failure_callback": alert_slack}

dag = DAG(
    dag_id="test_dag",
    default_args=args,
    schedule_interval=None,
    tags=["cdw"],
    catchup=False,
    description="The igloo-test-dag is intended to provide a place to experiment",
)


def test_task_1_fn():
    try:
        snowstorm_url = "https://hf9erhtu5i.execute-api.eu-west-1.amazonaws.com/prod/test"
        token_path = os.environ["AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"]
        token_host = "169.254.170.2"

        print("snowstorm url: {}, token path: {}, token host: {}".format(snowstorm_url, token_path, token_host))

        token_response = requests.get("http://{}{}".format(token_host, token_path))
        token = token_response.json()

        print("token: {}".format(token))

        access_key = token["AccessKeyId"]
        access_key_secret = token["SecretAccessKey"]
        session_token = token["Token"]

        print("access_key_id: {}, access_key_secret_id: {}".format(access_key, access_key_secret))

        auth = AWS4Auth(access_key, access_key_secret, "eu-west-1", "execute-api", session_token=session_token)

        response = requests.post(
            snowstorm_url,
            json={
                "data": "something",
            },
            auth=auth,
        )

        print(response.text)
        return True
    except Exception as e:
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush(5)
        raise e


# def test_task_2_fn(execution_date):
#     """
#     :param: execution_date a string in the form 'YYYY-MM-DD'
#     """
#     try:
#         raise Exception("An error occurred in test_task_2")
#     except Exception as e:
#         sentry_sdk.capture_exception(e)
#         sentry_sdk.flush(5)
#         raise e


# test_task_1 = PythonOperator(
#     task_id="test_task_1",
#     python_callable=test_task_1_fn,
#     op_args=["{{ ds }}"],
#     dag=dag,
#     description_md="""# Purpose

# This step extracts data from the Ensek API

# # Suggested action on failure

# No action

# # Justification

# This step has a very long run time, and the impact of failure is low. See [here](https://example.org) for more details.
# """,
# )

test_task_1 = PythonOperator(
    task_id="attempt-to-contact-snowstorm-queue-dag",
    python_callable=test_task_1_fn,
    dag=dag,
)

test_task_3 = BashOperator(task_id="print-env", bash_command="printenv", dag=dag)

test_task_4 = BashOperator(
    task_id="attempt-role-request", bash_command="curl 169.254.170.2$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", dag=dag
)
