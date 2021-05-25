import sys

sys.path.append("/opt/airflow/enzek-meterpoint-readings")

import datetime
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import sentry_sdk
import common
from common.process_glue_job import process_glue_job_await_completion
from common.slack_utils import alert_slack
from process_verification.verification_template import (
    verify_new_api_response_files_in_s3_directory,
    verify_seventeen_new_files_in_s3,
    ref_verification_step,
)
from process_Ensek import start_ensek_api_mirror_only_jobs
from conf import config
from common import schedules



dag_id = "ensek_main"

def get_schedule():
    env = config.environment_config['environment']
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
    tags=["cdw", "ensek",],
    catchup=False,
    description="Runs all ETLs for Ensek",
    max_active_runs=1,
)


directory = common.utils.get_dir()
s3_key = directory["s3_key"]
s3_bucket = directory["s3_bucket"]
environment = common.utils.get_env()
staging_job_name = directory["glue_staging_internalreadings_job_name"]
ref_job_name = directory["glue_ref_internalreadings_job_name"]

date_today_string = str(datetime.date.today())

# Slack Report Templates

task_slack_report_string = """
*Purpose*: {0}
*Failure Remediation*: {1}
*Justification*: {2}
"""

api_verification_report_string = task_slack_report_string.format(
    "Verifies there are sufficient new files in the s3 directory: {}, demonstrating the success of this API extracts",
    "Investigate signficance of verifcation failure in Airflow logs",
    "Failure indicates no data present in ETL destination"
)

api_extract_report_string = task_slack_report_string.format(
    "Api extraction for {}",
    "Do not rerun, investigate root cause.",
    "This step has a long run time, and rerunning it would likely impact other batch processes and Ensek response times.",
)


staging_report_string = task_slack_report_string.format(
    "Runs glue job on the stage 1 {} files which reduces them to 17 parquet files and writes them to stage 2",
    "Do not rerun, investigate root cause.",
    "Given this task is long running, rerunning it would impact other batch processes significantly.",
    )

ref_report_string = task_slack_report_string.format(
    "Runs glue job on stage 2 {} files which updates respective Redshift tables.",
    "Do not rerun, investigate root cause.",
    "Given this task is long running, rerunning it would impact other batch processes significantly.",
    )

staging_verify_report_string = task_slack_report_string.format(
    "Verifies there are sufficient new files in the staging directory.",
    "Do not rerun, investigate root cause.",
    "Rerunning will have the same outcome until valid data is present."
)


ref_verify_report_string = task_slack_report_string.format(
    "Verifies there is valid data in relevant redshift tables.",
    "Do not rerun, investigate root cause.",
    "Rerunning will have the same outcome until valid data is present."
)

# DAG Tasks

# Meterpoints Tasks

api_extract_meterpoints = BashOperator(
    task_id="api_extract_meterpoints_bash",
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_Ensek/processEnsekMeterpoints && python process_ensek_meterpoints_no_history.py",
    dag=dag,
)
api_extract_meterpoints.doc = api_extract_report_string.format("Meterpoint data for each account ID.")


api_extract_verify_meterpoints = PythonOperator(
    task_id="api_extract_verify_meterpoints",
    python_callable=verify_new_api_response_files_in_s3_directory,
    op_kwargs={
        "search_filter": date_today_string,
        "expected_value": "{{ var.value.LIVE_ACCOUNTS_FOR_VERIFY }}",
        "percent": "{{ var.value.PERCENT_FOR_API_VERIFY }}",
        "s3_prefix": s3_key["MeterPoints"],
    },
    dag=dag,
)
api_extract_verify_meterpoints.doc = api_verification_report_string.format(
    s3_key["MeterPoints"]
)


api_extract_verify_meterpoints_attributes = PythonOperator(
    task_id="api_extract_verify_meterpoints_attributes",
    python_callable=verify_new_api_response_files_in_s3_directory,
    op_kwargs={
        "search_filter": date_today_string,
        "expected_value": "{{ var.value.LIVE_ACCOUNTS_FOR_VERIFY }}",
        "percent": "{{ var.value.PERCENT_FOR_API_VERIFY }}",
        "s3_prefix": s3_key["MeterPointsAttributes"],
    },
    dag=dag,
)
api_extract_verify_meterpoints.doc = api_verification_report_string.format(
    s3_key["MeterPointsAttributes"]
)


api_extract_verify_meters = PythonOperator(
    task_id="api_extract_verify_meters",
    python_callable=verify_new_api_response_files_in_s3_directory,
    op_kwargs={
        "search_filter": date_today_string,
        "expected_value": "{{ var.value.LIVE_ACCOUNTS_FOR_VERIFY }}",
        "percent": "{{ var.value.PERCENT_FOR_API_VERIFY }}",
        "s3_prefix": s3_key["Meters"],
    },
    dag=dag,
)
api_extract_verify_meters.doc = api_verification_report_string.format(
    s3_key["Meters"]
)


api_extract_verify_meters_attributes = PythonOperator(
    task_id="api_extract_verify_meters_attributes",
    python_callable=verify_new_api_response_files_in_s3_directory,
    op_kwargs={
        "search_filter": date_today_string,
        "expected_value": "{{ var.value.LIVE_ACCOUNTS_FOR_VERIFY }}",
        "percent": "{{ var.value.PERCENT_FOR_API_VERIFY }}",
        "s3_prefix": s3_key["MetersAttributes"],
    },
    dag=dag,
)
api_extract_verify_meters_attributes.doc = api_verification_report_string.format(
    s3_key["MetersAttributes"]
)


api_extract_verify_registers = PythonOperator(
    task_id="api_extract_verify_registers",
    python_callable=verify_new_api_response_files_in_s3_directory,
    op_kwargs={
        "search_filter": date_today_string,
        "expected_value": "{{ var.value.LIVE_ACCOUNTS_FOR_VERIFY }}",
        "percent": "{{ var.value.PERCENT_FOR_API_VERIFY }}",
        "s3_prefix": s3_key["Registers"],
    },
    dag=dag,
)
api_extract_verify_registers.doc = api_verification_report_string.format(
    s3_key["Registers"]
)


api_extract_verify_register_attributes = PythonOperator(
    task_id="api_extract_verify_register_attributes",
    python_callable=verify_new_api_response_files_in_s3_directory,
    op_kwargs={
        "search_filter": date_today_string,
        "expected_value": "{{ var.value.LIVE_ACCOUNTS_FOR_VERIFY }}",
        "percent": "{{ var.value.PERCENT_FOR_API_VERIFY }}",
        "s3_prefix": s3_key["RegistersAttributes"],
    },
    dag=dag,
)
api_extract_verify_register_attributes.doc = api_verification_report_string.format(
    s3_key["RegistersAttributes"]
)

staging_meterpoints = PythonOperator(
    task_id="staging_meterpoints",
    python_callable=process_glue_job_await_completion,
    op_args=[staging_job_name, common.directories.common["meterpoints"]["glue_job_name_staging"]],
    dag=dag,
)
staging_meterpoints.doc = staging_report_string


staging_verify_meterpoints = PythonOperator(
    task_id="staging_verify_meterpoints",
    python_callable=verify_seventeen_new_files_in_s3,
    op_kwargs={"s3_prefix": "stage2/stage2_meterpoints"},
    dag=dag,
)
staging_verify_meterpoints.doc = staging_verify_report_string


ref_tables_meterpoints = PythonOperator(
    task_id="ref_tables_meterpoints",
    python_callable=process_glue_job_await_completion,
    op_args=[ref_job_name, common.directories.common["meterpoints"]["glue_job_name_ref"]],
    dag=dag,
)
ref_tables_meterpoints.doc = ref_report_string


ref_tables_verify_meterpoints = PythonOperator(
    task_id="ref_tables_verify_meterpoints",
    python_callable=ref_verification_step,
    op_kwargs={
        "ref_meterpoints": "ref_meterpoints",
        "ref_meterpoints_attributes": "ref_meterpoints_attributes",
        "ref_meters": "ref_meters",
        "ref_meters_attributes": "ref_meters_attributes",
        "ref_registers": "ref_registers",
        "ref_registers_attributes": "ref_registers_attributes",
    },
    dag=dag,
)
ref_tables_verify_meterpoints.doc = ref_verify_report_string


# Internal Readings tasks

api_extract_internalreadings = BashOperator(
    task_id="api_extract_internalreadings_bash",
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_Ensek/processEnsekReadings && python process_ensek_internal_readings.py",
    dag=dag,
)
api_extract_internalreadings.doc = api_extract_report_string.format("Internal Readings data per account ID")


api_extract_verify_internalreadings = PythonOperator(
    task_id="api_extract_verify_internalreadings",
    python_callable=verify_new_api_response_files_in_s3_directory,
    op_kwargs={
        "search_filter": date_today_string,
        "expected_value": "{{ var.value.LIVE_ACCOUNTS_FOR_VERIFY }}",
        "percent": "{{ var.value.PERCENT_FOR_API_VERIFY }}",
        "s3_prefix": s3_key["ReadingsInternal"],
    },
    dag=dag,
)
api_extract_verify_internalreadings.doc = api_verification_report_string.format(
    s3_key["ReadingsInternal"]
)

staging_internalreadings = PythonOperator(
    task_id="staging_internalreadings",
    python_callable=process_glue_job_await_completion,
    op_args=[staging_job_name, common.directories.common["internalreadings"]["glue_job_name_staging"]],
    dag=dag,
)
staging_internalreadings.doc = staging_report_string.format("Internal Readings")


staging_verify_internalreadings = PythonOperator(
    task_id="staging_verify_internalreadings",
    python_callable=verify_seventeen_new_files_in_s3,
    op_kwargs={"s3_prefix": common.directories.common["s3_keys"]["internal_readings_stage2"]},
    dag=dag,
)
staging_verify_internalreadings.doc = staging_verify_report_string


ref_tables_internalreadings = PythonOperator(
    task_id="ref_tables_internalreadings",
    python_callable=process_glue_job_await_completion,
    op_args=[ref_job_name, common.directories.common["internalreadings"]["glue_job_name_ref"]],
    dag=dag,
)
ref_tables_internalreadings.doc = ref_report_string


ref_tables_verify_internalreadings = PythonOperator(
    task_id="ref_tables_verify_internalreadings",
    python_callable=ref_verification_step,
    op_kwargs={
        "ref_readings_internal": "ref_readings_internal",
    },
    dag=dag,
)
ref_tables_verify_internalreadings.doc = ref_verify_report_string

process_customerdb = BashOperator(
    task_id='process_customerdb',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_Ensek && python start_customerdb_jobs.py',
    dag=dag,
)

# If preprod or dev then mirror

startensekjobs = start_ensek_api_mirror_only_jobs.StartEnsekJobs()

if environment in ["preprod", "dev"]:
    s3_destination_bucket = startensekjobs.dir["s3_bucket"]
    s3_source_bucket = startensekjobs.dir["s3_source_bucket"]

    ensek_meterpoints_mirror = PythonOperator(
        task_id="ensek_meterpoints_mirror",
        python_callable=startensekjobs.submit_process_s3_mirror_job,
        op_kwargs={
            "source_input": "s3://" + s3_source_bucket + "/stage1/MeterPoints/",
            "destination_input": "s3://"
            + s3_destination_bucket
            + "/stage1/MeterPoints/",
        },
        dag=dag,
    )

    ensek_meterpoints_attributes_mirror = PythonOperator(
        task_id="ensek_meterpoints_attributes_mirror",
        python_callable=startensekjobs.submit_process_s3_mirror_job,
        op_kwargs={
            "source_input": "s3://"
            + s3_source_bucket
            + "/stage1/MeterPointsAttributes/",
            "destination_input": "s3://"
            + s3_destination_bucket
            + "/stage1/MeterPointsAttributes/",
        },
        dag=dag,
    )

    ensek_meters_mirror = PythonOperator(
        task_id="ensek_meters_mirror",
        python_callable=startensekjobs.submit_process_s3_mirror_job,
        op_kwargs={
            "source_input": "s3://" + s3_source_bucket + "/stage1/Meters/",
            "destination_input": "s3://" + s3_destination_bucket + "/stage1/Meters/",
        },
        dag=dag,
    )

    ensek_meters_attributes_mirror = PythonOperator(
        task_id="ensek_meters_attributes_mirror",
        python_callable=startensekjobs.submit_process_s3_mirror_job,
        op_kwargs={
            "source_input": "s3://" + s3_source_bucket + "/stage1/MetersAttributes/",
            "destination_input": "s3://"
            + s3_destination_bucket
            + "/stage1/MetersAttributes/",
        },
        dag=dag,
    )

    ensek_registers_mirror = PythonOperator(
        task_id="ensek_registers_mirror",
        python_callable=startensekjobs.submit_process_s3_mirror_job,
        op_kwargs={
            "source_input": "s3://" + s3_source_bucket + "/stage1/Registers/",
            "destination_input": "s3://" + s3_destination_bucket + "/stage1/Registers/",
        },
        dag=dag,
    )

    ensek_registers_attributes_mirror = PythonOperator(
        task_id="ensek_registers_attributes_mirror",
        python_callable=startensekjobs.submit_process_s3_mirror_job,
        op_kwargs={
            "source_input": "s3://" + s3_source_bucket + "/stage1/RegistersAttributes/",
            "destination_input": "s3://"
            + s3_destination_bucket
            + "/stage1/RegistersAttributes/",
        },
        dag=dag,
    )

    ensek_readingsinternal_mirror = PythonOperator(
        task_id="ensek_readingsinternal_mirror",
        python_callable=startensekjobs.submit_process_s3_mirror_job,
        op_kwargs={
            "source_input": "s3://" + s3_source_bucket + "/stage1/ReadingsInternal/",
            "destination_input": "s3://"
            + s3_destination_bucket
            + "/stage1/ReadingsInternal/",
        },
        dag=dag,
    )


    # Not Prod Dependencies

    process_customerdb >> api_extract_meterpoints
    api_extract_meterpoints >> staging_meterpoints >> ref_tables_meterpoints
    api_extract_meterpoints >> ensek_meterpoints_mirror >> api_extract_verify_meterpoints
    api_extract_meterpoints >> ensek_meterpoints_attributes_mirror >> api_extract_verify_meterpoints_attributes
    api_extract_meterpoints >> ensek_meters_mirror >> api_extract_verify_meters
    api_extract_meterpoints >> ensek_meters_attributes_mirror >> api_extract_verify_meters_attributes
    api_extract_meterpoints >> ensek_registers_mirror >> api_extract_verify_registers
    api_extract_meterpoints >> ensek_registers_attributes_mirror >> api_extract_verify_register_attributes
    staging_meterpoints >> staging_verify_meterpoints
    ref_tables_meterpoints >> ref_tables_verify_meterpoints

    api_extract_meterpoints >> api_extract_internalreadings

    api_extract_internalreadings >> ensek_readingsinternal_mirror >> staging_internalreadings >> ref_tables_internalreadings
    ensek_readingsinternal_mirror >> api_extract_verify_internalreadings
    staging_internalreadings >> staging_verify_internalreadings
    ref_tables_internalreadings >> ref_tables_verify_internalreadings


else:
    # Dependencies
    process_customerdb >> api_extract_meterpoints

    api_extract_meterpoints >> staging_meterpoints >> ref_tables_meterpoints
    api_extract_meterpoints >> api_extract_verify_meterpoints
    api_extract_meterpoints >> api_extract_verify_meterpoints_attributes
    api_extract_meterpoints >> api_extract_verify_meters
    api_extract_meterpoints >> api_extract_verify_meters_attributes
    api_extract_meterpoints >> api_extract_verify_registers
    api_extract_meterpoints >> api_extract_verify_register_attributes
    staging_meterpoints >> staging_verify_meterpoints
    ref_tables_meterpoints >> ref_tables_verify_meterpoints

    api_extract_meterpoints >> api_extract_internalreadings

    api_extract_internalreadings >> staging_internalreadings >> ref_tables_internalreadings
    api_extract_internalreadings >> api_extract_verify_internalreadings
    staging_internalreadings >> staging_verify_internalreadings
    ref_tables_internalreadings >> ref_tables_verify_internalreadings