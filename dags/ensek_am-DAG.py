import sys
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.models import Variable

import datetime
from cdw.conf import config
from cdw.common import schedules
from cdw.common.utils import get_sla_timedelta
from cdw.common.slack_utils import alert_slack, post_slack_sla_alert
from cdw.common.process_glue_job import run_glue_job_await_completion
from cdw.process_verification.verification_template import (
    verify_files_in_s3_directory,
    verify_number_of_rows_in_table_greater_than,
)
import cdw.common
from cdw.conf import config
from cdw.common import schedules
from cdw.process_Ensek import start_ensek_api_mirror_only_jobs

dag_id = "ensek_am"
env = config.environment_config["environment"]


def get_schedule():
    return schedules.get_schedule(env, dag_id)


directory = cdw.common.utils.get_dir()
s3_key = directory["s3_key"]
s3_bucket = directory["s3_bucket"]
environment = cdw.common.utils.get_env()

date_today_string = str(datetime.date.today())

args = {
    "owner": "Airflow",
    "start_date": days_ago(2),
    "on_failure_callback": alert_slack,
    "sla": get_sla_timedelta(dag_id),
}

dag = DAG(
    dag_id=dag_id,
    default_args=args,
    schedule_interval=get_schedule(),
    sla_miss_callback=post_slack_sla_alert,
    tags=["cdw"],
    catchup=False,
    max_active_runs=1,
)

# sensor = ExternalTaskSensor(
#     task_id="ensek_main_finish_sensor_task",
#     external_dag_id="ensek_main",
#     external_task_id="api_extract_internalreadings_bash",
#     execution_date=datetime.datetime.today().strftime("%Y-%m-%d"),
#     start_date=days_ago(2),
# )

# configuring info for the dag run

# array should be in order you want the tasks items iterated over
# python maintains order of arrays when iterating

dag_data = {
    "process_ensek_registration_meterpoint_status": {
        "api_bash_command": "processEnsekStatus/process_ensek_registration_meterpoint_status.py",
        "glue_staging_job_name": "process_staging_ensek_registration_meterpoint_status_files",
        "glue_staging_job_process_parameter": "reg-mp-status",
        "glue_ref_job_name": "_process_ensek_registration_meterpoint_status_ref_tables",
        "glue_ref_job_process_parameter": "reg_mp_status",
        "s3_prefixes": ["RegistrationsElecMeterpoint", "RegistrationsGasMeterpoint"],
        "output_tables": [
            "ref_registrations_meterpoints_status_elec",
            "ref_registrations_meterpoints_status_elec_audit",
            "ref_registrations_meterpoints_status_gas",
            "ref_registrations_meterpoints_status_gas_audit",
        ],
    },
    "process_ensek_internal_estimates": {
        "api_bash_command": "processEnsekEstimates/process_ensek_internal_estimates.py",
        "glue_staging_job_name": "process_staging_ensek_internal_estimates_files",
        "glue_staging_job_process_parameter": "ensek-internal-estimates",
        "glue_ref_job_name": "_process_ensek_internal_estimates_ref_tables",
        "glue_ref_job_process_parameter": "ensek_ref_estimates",
        "s3_prefixes": ["EstimatesElecInternal", "EstimatesGasInternal"],
        "output_tables": [
            "ref_estimates_elec_internal",
            "ref_estimates_elec_internal_audit",
            "ref_estimates_gas_internal",
            "ref_estimates_gas_internal_audit",
        ],
    },
    "process_ensek_tariffs_history": {
        "api_bash_command": "processEnsekTariffs/process_ensek_tariffs_history.py {{ var.value.TARIFF_WITH_LIVE_MISMATCH_BOOLEAN }}",
        "glue_staging_job_name": "process_staging_ensek_tariffs_history_files",
        "glue_staging_job_process_parameter": "ensek-tariffs",
        "glue_ref_job_name": "_process_ensek_tariffs_history_ref_tables",
        "glue_ref_job_process_parameter": "ensek_ref_tariffs",
        "s3_prefixes": [
            "TariffHistory",
            "TariffHistoryElecStandCharge",
            "TariffHistoryElecUnitRates",
            "TariffHistoryGasStandCharge",
            "TariffHistoryGasUnitRates",
        ],
        "output_tables": [
            "ref_tariff_history",
            "ref_tariff_history_audit",
            "ref_tariff_history_elec_sc",
            "ref_tariff_history_elec_sc_audit",
            "ref_tariff_history_elec_ur",
            "ref_tariff_history_elec_ur_audit",
            "ref_tariff_history_gas_sc",
            "ref_tariff_history_gas_sc_audit",
            "ref_tariff_history_gas_ur",
            "ref_tariff_history_gas_ur_audit",
        ],
    },
    "process_ensek_account_settings": {
        "api_bash_command": "processEnsekAccountSettings/process_ensek_account_settings.py",
        "glue_staging_job_name": "process_staging_ensek_account_settings_files",
        "glue_staging_job_process_parameter": "ensek-account-settings",
        "s3_prefixes": ["AccountSettings"],
        "output_tables": [],
    },
    "process_ensek_transactions": {
        "api_bash_command": "processEnsekTransactions/process_ensek_transactions.py",
        "glue_staging_job_name": "process_staging_ensek_transactions_files",
        "glue_staging_job_process_parameter": "ensek-transactions",
        "glue_ref_job_name": "_process_ensek_transactions_ref_tables",
        "glue_ref_job_process_parameter": "ensek_acc_trans",
        "s3_prefixes": ["AccountTransactions"],
        "output_tables": ["ref_account_transactions"],
    },
}

directory = cdw.common.utils.get_dir()
s3_key = directory["s3_key"]
s3_bucket = directory["s3_bucket"]
environment = cdw.common.utils.get_env()

date_today_string = str(datetime.date.today())


def createS3VerificationStep(task_id, path, task_description, expected_value):
    """
    A Wrapper function for our verification step. One which checks to verify we have enough files in a given S3 location
    """
    return PythonOperator(
        task_id=task_id,
        python_callable=verify_files_in_s3_directory,
        op_kwargs={
            "search_filter": date_today_string,
            "expected_value": expected_value,
            "s3_prefix": path,
        },
        dag=dag,
        doc_md=task_description,
    )


# ideally this variable would come from yesterday's counts for now will stay hardcoded
number_of_rows = 10


def createRefVerificationStep(task_id, table_name):
    return PythonOperator(
        task_id=task_id,
        python_callable=verify_number_of_rows_in_table_greater_than,
        op_kwargs={"table_name": table_name, "expected_count": number_of_rows},
        dag=dag,
    )


task_slack_report_string = """
*Purpose*: {0}
*Failure Remediation*: {1}
*Justification*: {2}
"""

# non iterable tasks
# dependencies for these tasks created at the bottom of the script

igloo_alp_trigger = TriggerDagRunOperator(task_id="igloo_alp_trigger", trigger_dag_id="igloo_alp", dag=dag)
igloo_alp_trigger.doc = task_slack_report_string.format(
    "Triggers the ALP DAG",
    "Investigate root cause and rerun depedending.",
    "This step only triggers another DAG, and is only likely to fail because of airflow config.",
)
# igloo_alp_trigger.set_upstream(sensor)


tasks = {}
last_api_extract_operator = ""

for datatype, datatype_info in dag_data.items():

    # Creates api download tasks
    api_task_string = f"api_extract_{datatype}_bash"
    tasks[api_task_string] = BashOperator(
        task_id=api_task_string,
        bash_command="cd /opt/airflow/cdw/process_Ensek && python {}".format(datatype_info["api_bash_command"]),
        dag=dag,
        doc_md=task_slack_report_string.format(
            f"Api extraction for {datatype}",
            "Do not rerun, investigate root cause.",
            "This step has a long run time, and rerunning it would likely impact other batch processes and Ensek response times.",
        ),
    )

    if last_api_extract_operator != "":
        tasks[api_task_string].set_upstream(last_api_extract_operator)
    last_api_extract_operator = tasks[api_task_string]
    # last_api_extract_operator.set_upstream(sensor)

    # Creates staging tasks
    staging_task_string = datatype.replace("process", "staging")
    tasks[staging_task_string] = PythonOperator(
        task_id=staging_task_string,
        python_callable=run_glue_job_await_completion,
        op_args=[
            datatype_info["glue_staging_job_name"],
            datatype_info["glue_staging_job_process_parameter"],
        ],
        dag=dag,
        doc_md=task_slack_report_string.format(
            f"Runs glue job on the stage 1 {datatype}, files which reduces them to 17 parquet files and writes them to stage 2",
            "Do not rerun until root cause is understood, data will correct overnight given cause is resolved.",
            "Rerunning could cause other tasks to finish later than required.",
        ),
    )

    # Creates ref tasks
    if "glue_ref_job_name" in datatype_info:
        ref_task_string = datatype.replace("process", "ref")

        tasks[ref_task_string] = PythonOperator(
            task_id=ref_task_string,
            python_callable=run_glue_job_await_completion,
            op_args=[
                datatype_info["glue_ref_job_name"],
                datatype_info["glue_ref_job_process_parameter"],
            ],
            dag=dag,
            doc_md=task_slack_report_string.format(
                f"Runs glue job on stage 2 {datatype} files which updates respective Redshift tables.",
                "Do not rerun, investigate root cause.",
                "Given this task is long running, rerunning it would impact other batch processes significantly.",
            ),
        )
        tasks[ref_task_string].set_upstream(tasks[staging_task_string])

    tasks[staging_task_string].set_upstream(tasks[api_task_string])

    # Creates verification tasks
    for prefix in datatype_info["s3_prefixes"]:

        api_verify_string = f"api_extract_verify_{prefix}"
        verify_task_docstring_stage1 = task_slack_report_string.format(
            f"Verifies there are sufficient new files in the s3 directory: {prefix}, demonstrating the success of this API extraction",
            "Investigate signficance of verifcation failure in Airflow logs",
            "Failure indicates inaccurate amounts of data in ETL destination",
        )
        verify_task_docstring_stage2 = task_slack_report_string.format(
            f"Verifies there are sufficient new files in the stage2 directory for: {prefix}.",
            "Do not rerun, investigate root cause.",
            "Rerunning will have the same outcome until valid data is present.",
        )
        # stage1
        tasks[api_verify_string] = createS3VerificationStep(
            api_verify_string,
            "stage1/{}".format(prefix),
            verify_task_docstring_stage1,
            int(Variable.get("STAGE1_VERIFICATION_THRESHOLD")),
        ).set_upstream(tasks[api_task_string])

        # stage2
        stage2_verify_string = f"stage2_verify_{prefix}"
        tasks[stage2_verify_string] = createS3VerificationStep(
            stage2_verify_string,
            "stage2/stage2_{}".format(prefix),
            verify_task_docstring_stage2,
            int(Variable.get("STAGE2_VERIFICATION_THRESHOLD")),
        ).set_upstream(tasks[staging_task_string])

        # ref
    for table in datatype_info["output_tables"]:
        ref_verify_docstring = task_slack_report_string.format(
            f"Verifies there is valid data in redshift table: {table}.",
            "Do not rerun, investigate root cause.",
            "Rerunning will have the same outcome until valid data is present.",
        )
        table_string = f"verify_populated_{table}"
        tasks[table_string] = createRefVerificationStep(table_string, table).set_upstream(tasks[ref_task_string])

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
                    "source_input": f"s3://{s3_source_bucket}/stage1/{path}/",
                    "destination_input": f"s3://{s3_destination_bucket}/stage1/{path}/",
                },
                dag=dag,
            )

        for prefix in datatype_info["s3_prefixes"]:
            mirror_task_string = f"mirroring_{datatype}_{prefix}"
            tasks[mirror_task_string] = createMirrorTask(mirror_task_string, prefix)
            tasks[staging_task_string].set_upstream(tasks[mirror_task_string])
            tasks[f"api_extract_verify_{prefix}"].set_upsream(tasks[mirror_task_string])
            # tasks[mirror_task_string].set_upstream(sensor)
