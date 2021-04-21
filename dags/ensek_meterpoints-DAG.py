import sys

sys.path.append("/opt/airflow/enzek-meterpoint-readings")

import datetime
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import sentry_sdk
from common.slack_utils import alert_slack
from process_verification.verification_template import dummy_verification_task, VerifcationTask, api_extract_verify_meterpoints_object


args = {
    "owner": "Airflow",
    "start_date": days_ago(2),
    "on_failure_callback": alert_slack,
}

# Used to give better visisbility in the UI, would be nice if this could be a variable coming from elsewhere rather than a hardcoded string
dag_description = "This DAG is responsible for extracting meterpoint information for each account ID from ensek's API, upon failure it is likely a selection of account IDs will have not have meterpoint data updated from ensek"


# For DAG testing
def dummy_python_task():
    print("I am a dummy python task")



dag = DAG(
    dag_id="ensek_meterpoints",
    default_args=args,
    schedule_interval=None,
    tags=["cdw", "example","ensek","meterpoints"],
    catchup=False,
    description=dag_description,
)



def sentry_wrapper(python_task_variable):
    """
    :param: execution_date a string in the form 'YYYY-MM-DD'
    """
    try:
        python_task_variable
    except Exception as e:
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush(5)
        raise e


api_extract_meterpoints = PythonOperator(
    task_id="api_extract_meterpoints",
    python_callable=sentry_wrapper,
    op_args=[dummy_python_task],
    dag=dag,
)


api_extract_verify_meterpoints = PythonOperator(
    task_id="api_extract_verify_meterpoints",
    python_callable=sentry_wrapper,
    op_args=[dummy_verification_task],
    dag=dag,
)

api_extract_verify_meterpoints.doc_md =  api_extract_verify_meterpoints_object.description


api_extract_verify_meterpoints_attributes = PythonOperator(
    task_id="api_extract_verify_meterpoints_attributes",
    python_callable=sentry_wrapper,
    op_args=[dummy_verification_task],
    dag=dag,
)



api_extract_verify_meters = PythonOperator(
    task_id="api_extract_verify_meters",
    python_callable=sentry_wrapper,
    op_args=[dummy_verification_task],
    dag=dag,
)

api_extract_verify_meters_attributes = PythonOperator(
    task_id="api_extract_verify_meters_attributes",
    python_callable=sentry_wrapper,
    op_args=[dummy_verification_task],
    dag=dag,
)

api_extract_verify_registers = PythonOperator(
    task_id="api_extract_verify_registers",
    python_callable=sentry_wrapper,
    op_args=[dummy_verification_task],
    dag=dag,
)

api_extract_verify_register_attributes = PythonOperator(
    task_id="api_extract_verify_register_attributes",
    python_callable=sentry_wrapper,
    op_args=[dummy_verification_task],
    dag=dag,
)

staging_meterpoints = PythonOperator(
    task_id="staging_meterpoints",
    python_callable=sentry_wrapper,
    op_args=[dummy_python_task],
    dag=dag,
)

staging_verify_meterpoints = PythonOperator(
    task_id="staging_verify_meterpoints",
    python_callable=sentry_wrapper,
    op_args=[dummy_verification_task],
    dag=dag,
)

ref_tables_meterpoints = PythonOperator(
    task_id="ref_tables_meterpoints",
    python_callable=sentry_wrapper,
    op_args=[dummy_python_task],
    dag=dag,
)

ref_tables_verify_meterpoints = PythonOperator(
    task_id="ref_tables_verify_meterpoints",
    python_callable=sentry_wrapper,
    op_args=[dummy_verification_task],
    dag=dag,
)



api_extract_meterpoints >> staging_meterpoints >> ref_tables_meterpoints
api_extract_meterpoints >> api_extract_verify_meterpoints
api_extract_meterpoints >> api_extract_verify_meterpoints_attributes
api_extract_meterpoints >> api_extract_verify_meters
api_extract_meterpoints >> api_extract_verify_meters_attributes
api_extract_meterpoints >> api_extract_verify_registers
api_extract_meterpoints >> api_extract_verify_register_attributes
staging_meterpoints >> staging_verify_meterpoints
ref_tables_meterpoints >> ref_tables_verify_meterpoints
