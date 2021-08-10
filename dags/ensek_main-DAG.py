import sys

sys.path.append("/opt/airflow/enzek-meterpoint-readings")

import datetime
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import common
from common.utils import get_sla_timedelta
from common.process_glue_job import run_glue_job_await_completion
from common.slack_utils import alert_slack, post_slack_sla_alert
from process_verification.verification_template import (
    verify_new_api_response_files_in_s3_directory,
    verify_seventeen_new_files_in_s3,
    ref_verification_step,
)
from process_Ensek import start_ensek_api_mirror_only_jobs
from conf import config
from common import schedules

dag_id = "ensek_main"
env = config.environment_config["environment"]


def get_schedule():
    return schedules.get_schedule(env, dag_id)


dag = DAG(
    dag_id=dag_id,
    default_args={
        "owner": "Airflow",
        "start_date": days_ago(2),
        "on_failure_callback": alert_slack,
        "sla": get_sla_timedelta(dag_id),
    },
    schedule_interval=get_schedule(),
    sla_miss_callback=post_slack_sla_alert,
    tags=[
        "cdw",
        "ensek",
    ],
    catchup=False,
    description="Runs all ETLs for Ensek",
    max_active_runs=1,
)

directory = common.utils.get_dir()
s3_key = directory["s3_key"]
s3_bucket = directory["s3_bucket"]
environment = common.utils.get_env()

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
    "Failure indicates no data present in ETL destination",
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
    "Rerunning will have the same outcome until valid data is present.",
)

ref_verify_report_string = task_slack_report_string.format(
    "Verifies there is valid data in relevant redshift tables.",
    "Do not rerun, investigate root cause.",
    "Rerunning will have the same outcome until valid data is present.",
)

api_extract_meterpoints = BashOperator(
    task_id="api_extract_meterpoints_bash",
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_Ensek/processEnsekMeterpoints && python process_ensek_meterpoints_no_history.py",
    dag=dag,
)
api_extract_meterpoints.doc = api_extract_report_string.format("Meterpoint data for each account ID.")


def createS3VerificationStep(task_id, path):
    """
    A Wrapper function for our verification step. One which checks to verify we have enough files in a given S3 location
    """
    return PythonOperator(
        task_id=task_id,
        python_callable=verify_new_api_response_files_in_s3_directory,
        op_kwargs={
            "search_filter": date_today_string,
            "expected_value": "{{ var.value.LIVE_ACCOUNTS_FOR_VERIFY }}",
            "percent": "{{ var.value.PERCENT_FOR_API_VERIFY }}",
            "s3_prefix": s3_key[path],
        },
        dag=dag,
    )


api_extract_verify_meterpoints = createS3VerificationStep("api_extract_verify_meterpoints", "MeterPoints")

api_extract_verify_meterpoints.doc = api_verification_report_string.format(s3_key["MeterPoints"])

api_extract_verify_meterpoints_attributes = createS3VerificationStep(
    "api_extract_verify_meterpoints_attributes", "MeterPointsAttributes"
)

api_extract_verify_meterpoints.doc = api_verification_report_string.format(s3_key["MeterPointsAttributes"])

api_extract_verify_meters = createS3VerificationStep("api_extract_verify_meters", "Meters")

api_extract_verify_meters.doc = api_verification_report_string.format(s3_key["Meters"])

api_extract_verify_meters_attributes = createS3VerificationStep(
    "api_extract_verify_meters_attributes", "MetersAttributes"
)

api_extract_verify_meters_attributes.doc = api_verification_report_string.format(s3_key["MetersAttributes"])

api_extract_verify_registers = createS3VerificationStep("api_extract_verify_registers", "Registers")

api_extract_verify_registers.doc = api_verification_report_string.format(s3_key["Registers"])

api_extract_verify_register_attributes = createS3VerificationStep(
    "api_extract_verify_register_attributes", "RegistersAttributes"
)

api_extract_verify_register_attributes.doc = api_verification_report_string.format(s3_key["RegistersAttributes"])

staging_meterpoints = PythonOperator(
    task_id="staging_meterpoints",
    python_callable=run_glue_job_await_completion,
    op_args=[
        directory["glue_staging_meterpoints_job_name"],
        common.directories.common["meterpoints"]["glue_job_name_staging"],
    ],
    dag=dag,
)
staging_meterpoints.doc = staging_report_string.format("Meterpoints")

staging_verify_meterpoints = PythonOperator(
    task_id="staging_verify_meterpoints",
    python_callable=verify_seventeen_new_files_in_s3,
    op_kwargs={"s3_prefix": "stage2/stage2_meterpoints"},
    dag=dag,
)
staging_verify_meterpoints.doc = staging_verify_report_string

ref_tables_meterpoints = PythonOperator(
    task_id="ref_tables_meterpoints",
    python_callable=run_glue_job_await_completion,
    op_args=[directory["glue_ref_meterpoints_job_name"], common.directories.common["meterpoints"]["glue_job_name_ref"]],
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

api_extract_verify_internalreadings = createS3VerificationStep(
    "verify_new_api_response_files_in_s3_directory", "ReadingsInternal"
)

api_extract_verify_internalreadings.doc = api_verification_report_string.format(s3_key["ReadingsInternal"])

staging_internalreadings = PythonOperator(
    task_id="staging_internalreadings",
    python_callable=run_glue_job_await_completion,
    op_args=[
        directory["glue_staging_internalreadings_job_name"],
        common.directories.common["internalreadings"]["glue_job_name_staging"],
    ],
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
    python_callable=run_glue_job_await_completion,
    op_args=[
        directory["glue_ref_internalreadings_job_name"],
        common.directories.common["internalreadings"]["glue_job_name_ref"],
    ],
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
    task_id="process_customerdb",
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_Ensek && python start_customerdb_jobs.py",
    dag=dag,
)

# If preprod or dev then mirror

startensekjobs = start_ensek_api_mirror_only_jobs.StartEnsekJobs()

if environment in ["preprod", "dev"]:
    s3_destination_bucket = startensekjobs.dir["s3_bucket"]
    s3_source_bucket = startensekjobs.dir["s3_source_bucket"]

    def createMirrorTask(task_id, path):
        """
        Wrapper for mirror etl
        """
        return PythonOperator(
            task_id=task_id,
            python_callable=startensekjobs.submit_process_s3_mirror_job,
            op_kwargs={
                "source_input": "s3://{}/stage1/{}/".format(s3_source_bucket, path),
                "destination_input": "s3://{}/stage1/{}/".format(s3_destination_bucket, path),
            },
            dag=dag,
        )

    ensek_meterpoints_mirror = createMirrorTask("ensek_meterpoints_mirror", "MeterPoints")

    ensek_meterpoints_attributes_mirror = createMirrorTask(
        "ensek_meterpoints_attributes_mirror", "MeterPointsAttributes"
    )

    ensek_meters_mirror = createMirrorTask("ensek_meters_mirror", "Meters")

    ensek_meters_attributes_mirror = createMirrorTask("ensek_meters_attributes_mirror", "MetersAttributes")

    ensek_registers_mirror = createMirrorTask("ensek_registers_mirror", "Registers")

    ensek_registers_attributes_mirror = createMirrorTask("ensek_registers_attributes_mirror", "RegistersAttributes")

    ensek_readingsinternal_mirror = createMirrorTask("ensek_readingsinternal_mirror", "ReadingsInternal")

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

    (
        api_extract_internalreadings
        >> ensek_readingsinternal_mirror
        >> staging_internalreadings
        >> ref_tables_internalreadings
    )
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
