from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
import sys
import datetime
import sentry_sdk

sys.path.append("/opt/airflow/enzek-meterpoint-readings")
from process_smart.d0379 import generate_d0379, copy_d0379_to_sftp
from common.slack_utils import alert_slack
import common

from common.process_glue_crawler import run_glue_crawler
from common.process_glue_job import run_glue_job_await_completion
from conf import config
from common import schedules
from common import utils as util

dag_id = "SMART"


def get_schedule():
    env = config.environment_config["environment"]
    return schedules.get_schedule(env, dag_id)


def execute_redshift_sql_query(sql):
    """
    :param: SQL expression to execute
    Executes SQL expression and will pass any errors to Sentry
    """
    try:
        print("Running SQL --- \n   {}".format(sql))
        response = common.utils.execute_redshift_sql_query(sql)
        return response
    except Exception as e:
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush(5)
        raise e


def generate_d0379_wrapper(execution_date):
    """
    :param: execution_date a string in the form 'YYYY-MM-DD'
    """
    try:
        print("D0379 execution_date={}".format(execution_date))
        d0379_date = datetime.date.fromisoformat(execution_date)
        generate_d0379(d0379_date)
    except Exception as e:
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush(5)
        raise e


def copy_d0379_to_sftp_wrapper(execution_date):
    """
    :param: execution_date a string in the form 'YYYY-MM-DD'
    """
    try:
        print("copy_d0379_to_sftp execution_date={}".format(execution_date))
        d0379_date = datetime.date.fromisoformat(execution_date)
        copy_d0379_to_sftp(d0379_date)
    except Exception as e:
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush(5)
        raise e


args = {
    "owner": "Airflow",
    "start_date": days_ago(2),  # don't know what this is doing
    "on_failure_callback": alert_slack,
}

dag = DAG(
    dag_id=dag_id,
    default_args=args,
    schedule_interval=get_schedule(),
    tags=["cdw"],
    catchup=False,
    max_active_runs=1,
    description="Smart DAG 1.0.2",
)


stage_daily_gas_reads_asei = PythonOperator(
    task_id="stage_daily_gas_reads_asei",
    python_callable=run_glue_job_await_completion,
    op_args=[
        "process_staging_smart_files",
        "asei-smart-reads-4.6.1-gas",
    ],
    dag=dag,
    doc_md=""" *Purpose* Runs Glue Job for data described in task_id.
        *Remidiation* Escalate and assess best recovery process.
        *Justificaton* Upon failure it is likely stage 2 files have not been updated for this dataset.""",
)

stage_daily_elec_reads_asei = PythonOperator(
    task_id="stage_daily_elec_reads_asei",
    python_callable=run_glue_job_await_completion,
    op_args=[
        "process_staging_smart_files",
        "asei-smart-reads-4.6.1-elec",
    ],
    dag=dag,
    doc_md=""" *Purpose* Runs Glue Job for data described in task_id.
        *Remidiation* Escalate and assess best recovery process.
        *Justificaton* Upon failure it is likely stage 2 files have not been updated for this dataset.""",
)

stage_half_hourly_gas_reads_asei = PythonOperator(
    task_id="stage_half_hourly_gas_reads_asei",
    python_callable=run_glue_job_await_completion,
    op_args=[
        "process_staging_smart_files",
        "asei-smart-reads-4.8.1-gas",
    ],
    dag=dag,
    doc_md="""*Purpose* Runs Glue Job for data described in task_id.
        *Remidiation* Escalate and assess best recovery process.
        *Justificaton* Upon failure it is likely stage 2 files have not been updated for this dataset.""",
)

stage_half_hourly_elec_reads_asei = PythonOperator(
    task_id="stage_half_hourly_elec_reads_asei",
    python_callable=run_glue_job_await_completion,
    op_args=[
        "process_staging_smart_files",
        "asei-smart-reads-4.8.1-elec",
    ],
    dag=dag,
    doc_md=""" *Purpose* Runs Glue Job for data described in task_id.
        *Remidiation* Escalate and assess best recovery process.
        *Justificaton* Upon failure it is likely stage 2 files have not been updated for this dataset.""",
)

stage_smart_inventory = PythonOperator(
    task_id="stage_smart_inventory",
    python_callable=run_glue_job_await_completion,
    op_args=[
        "process_staging_smart_files",
        "smart-inventory",
    ],
    dag=dag,
    doc_md=""" *Purpose* Runs Glue Job for data described in task_id.
        *Remidiation* Escalate and assess best recovery process.
        *Justificaton* Upon failure it is likely stage 2 files have not been updated for this dataset.""",
)

smart_staging_task_list = [
    stage_smart_inventory,
    stage_daily_gas_reads_asei,
    stage_daily_elec_reads_asei,
    stage_half_hourly_gas_reads_asei,
    stage_half_hourly_elec_reads_asei,
]

stage_usmart_4_6_1_gas = PythonOperator(
    task_id="stage_usmart_4_6_1_gas",
    python_callable=run_glue_job_await_completion,
    op_args=[
        "process_staging_usmart_daily_files",
        "usmart-daily-gas",
    ],
    dag=dag,
    doc_md=""" *Purpose* Runs Glue Job for data described in task_id
        *Remidiation* Escalate and assess best recovery process
        *Justificaton* Upon failure it is likely stage 2 files have not been updated for this dataset""",
)

stage_usmart_4_6_1_elec = PythonOperator(
    task_id="stage_usmart_4_6_1_elec",
    python_callable=run_glue_job_await_completion,
    op_args=[
        "process_staging_usmart_daily_files",
        "usmart-daily-elec",
    ],
    dag=dag,
    doc_md=""" *Purpose* Runs Glue Job for data described in task_id
        *Remidiation* Escalate and assess best recovery process
        *Justificaton* Upon failure it is likely stage 2 files have not been updated for this dataset""",
)


# Executes glue job '_process_ref_tables' with processJob argument 'smart_reader_daily_reporting'
# 'smart_reader_daily_reporting' is defined in the 'report_groups' dict in process_ensek_data 'report_definitions.py'
# _process_ref_tables is the name of a glue job, it's script location is
# s3://${BucketName}/glue-scripts-py/process-ref-tables/_process_ref_tables.py
# it's default arguments are:
#         "--TempDir": !Sub
#             - s3://${BucketName}/aws-glue-tempdir"
#             - { BucketName: !ImportValue CoreIglooDataWarehouseS3Bucket }
#         "--job-bookmark-option": "job-bookmark-disable"
#         "--environment": !FindInMap [ EnvironmentMap, !Ref Environment, GlueEnvironment ]
#         "--job-language": "python"
#         "--process_job": "ensek_pa"
#         "--s3_bucket": !ImportValue CoreIglooDataWarehouseS3Bucket
#
# When the _process_ref_tables Glue job is executed by start_smart_all_reporting_jobs.py, it
# is passed the following arguments:
#
# response = self.glue_client.start_job_run(
#     JobName=self.job_name,
#     Arguments={
#         "--process_job": self.process_job,
#         "--s3_bucket": self.s3_bucket,
#         "--environment": self.environment,
#     },
# )
#
# So we can see that the actual arguments that will be in place will be:
#
# TempDir: s3://igloo-data-warehouse-prod-630944350233/aws-glue-tempdir
# job-bookmark-option: "job-bookmark-disable"
# environment: "newprod"
# job-language: "python"
# process_job: "smart_reader_daily_reporting"
# s3_bucket: "igloo-data-warehouse-prod-630944350233"
#
# This eventually ends up calling this piece of code in _process_ref_tables.py:
#
# def process_generic_report(_spark_op, report_definitions, report_group):
#   gr = rep_gen.GenericReporter(_spark_op)
#   for report_key in report_group:
#     rd = report_definitions[report_key]
#     gr.process_reporting_glue_job_from_dict(rd)
#     print(rd['report_name'] + ' processed')
#   print(_spark_op.process_job + ' completed')
#
# with report_group set to 'smart_reader_daily_reporting'
#
# 'smart_reader_daily_reporting' contains four reports:
# [
#   'readings_smart_daily_raw',
#   'readings_smart_daily',
#   'smart_inventory_raw',
#   'smart_inventory'
# ]
#
# So in summary, we can replace start_smart_all_ref_jobs with task with
# something that calls a glue job directly with the correct arguments,
# but really we should replace it with four distinct tasks, breaking down
# 'smart_reader_daily_reporting' in to discrete steps.
#

# start_smart_all_ref_jobs = BashOperator(
#     task_id="start_smart_all_ref_jobs",
#     bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_smart && python start_smart_all_reporting_jobs.py",
#     dag=dag,
# )

task_slack_report_string = """
*Purpose*: {0}
*Failure Remediation*: {1}
*Justification*: {2}
"""

populate_ref_readings_smart_daily_raw = PythonOperator(
    dag=dag,
    task_id="populate_ref_readings_smart_daily_raw",
    python_callable=run_glue_job_await_completion,
    op_args=["_process_ref_tables", "populate_ref_readings_smart_daily_raw"],
)

populate_ref_readings_smart_daily = PythonOperator(
    dag=dag,
    task_id="populate_ref_readings_smart_daily",
    python_callable=run_glue_job_await_completion,
    op_args=["_process_ref_tables", "populate_ref_readings_smart_daily"],
)

populate_ref_smart_inventory_raw = PythonOperator(
    dag=dag,
    task_id="populate_ref_smart_inventory_raw",
    python_callable=run_glue_job_await_completion,
    op_args=["_process_ref_tables", "populate_ref_smart_inventory_raw"],
)

populate_ref_smart_inventory = PythonOperator(
    dag=dag,
    task_id="populate_ref_smart_inventory",
    python_callable=run_glue_job_await_completion,
    op_args=["_process_ref_tables", "populate_ref_smart_inventory"],
)

start_smart_all_billing_reads_jobs = BashOperator(
    task_id="start_smart_all_billing_reads_jobs",
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_smart && python r2b.py --billing-window-start-date {{ tomorrow_ds }} {{ var.value.R2B_CLI_PARAMS }}",
    dag=dag,
)

refresh_mv_smart_stage2_smarthalfhourlyreads_elec = PythonOperator(
    dag=dag,
    task_id="refresh_mv_smart_stage2_smarthalfhourlyreads_elec",
    python_callable=execute_redshift_sql_query,
    op_args=["REFRESH MATERIALIZED VIEW mv_smart_stage2_smarthalfhourlyreads_elec"],
)

crawl_stage2_usmart_4_6_1_gas = PythonOperator(
    task_id="crawl_stage2_usmart_4_6_1_gas",
    python_callable=run_glue_crawler,
    op_args=["data-crawler-stage2-usmart-4.6.1-gas"],
    dag=dag,
)

crawl_stage2_usmart_4_6_1_elec = PythonOperator(
    task_id="crawl_stage2_usmart_4_6_1_elec",
    python_callable=run_glue_crawler,
    op_args=["data-crawler-stage2-usmart-4.6.1-elec"],
    dag=dag,
)

generate_d0379_task = PythonOperator(
    task_id="generate_d0379",
    python_callable=generate_d0379_wrapper,
    op_args=["{{ ds }}"],
    dag=dag,
)

copy_d0379_to_sftp_task = PythonOperator(
    task_id="copy_d0379_to_sftp",
    python_callable=copy_d0379_to_sftp_wrapper,
    op_args=["{{ ds }}"],
    dag=dag,
)

refresh_mv_readings_smart_daily_usmart = PythonOperator(
    task_id="refresh_mv_readings_smart_daily_usmart",
    python_callable=execute_redshift_sql_query,
    op_args=["REFRESH MATERIALIZED VIEW mv_readings_smart_daily_usmart"],
    dag=dag,
)

populate_ref_readings_smart_daily_uSmart_raw = PythonOperator(
    task_id="populate_ref_readings_smart_daily_uSmart_raw",
    python_callable=execute_redshift_sql_query,
    op_args=[
        """ TRUNCATE ref_readings_smart_daily_uSmart_raw;
            INSERT INTO ref_readings_smart_daily_uSmart_raw (SELECT cast(mpxn as bigint),
                                                            deviceid,
                                                            "type",
                                                            total_consumption,
                                                            register_num,
                                                            register_value,
                                                            "timestamp",
                                                            getdate()
                                                            FROM mv_readings_smart_daily_usmart)
            """
    ],
    dag=dag,
)

truncate_ref_readings_smart_daily_all = PythonOperator(
    dag=dag,
    task_id="truncate_ref_readings_smart_daily_all",
    python_callable=execute_redshift_sql_query,
    op_args=["""TRUNCATE ref_readings_smart_daily_all"""],
)

populate_ref_readings_smart_daily_all_with_usmart_reads = PythonOperator(
    dag=dag,
    task_id="populate_ref_readings_smart_daily_all_with_usmart_reads",
    python_callable=run_glue_job_await_completion,
    op_args=["_process_ref_tables", "populate_ref_readings_smart_daily_all_with_usmart_reads"],
)

populate_ref_readings_smart_daily_all_with_asei_elec_reads = PythonOperator(
    dag=dag,
    task_id="populate_ref_readings_smart_daily_all_with_asei_elec_reads",
    python_callable=run_glue_job_await_completion,
    op_args=["_process_ref_tables", "populate_ref_readings_smart_daily_all_with_asei_elec_reads"],
)

populate_ref_readings_smart_daily_all_with_asei_gas_reads = PythonOperator(
    dag=dag,
    task_id="populate_ref_readings_smart_daily_all_with_asei_gas_reads",
    python_callable=run_glue_job_await_completion,
    op_args=["_process_ref_tables", "populate_ref_readings_smart_daily_all_with_asei_gas_reads"],
)

legacy_asei_daily_smart_ref_jobs_complete = DummyOperator(
    dag=dag,
    task_id="legacy_asei_daily_smart_ref_jobs_complete",
)

legacy_asei_half_hourly_smart_ref_jobs_complete = DummyOperator(
    dag=dag,
    task_id="legacy_asei_half_hourly_smart_ref_jobs_complete",
)

# pylint: disable=pointless-statement,line-too-long
# fmt: off

# Legacy Read-to-Bill & Elective Half-hourly Settlement Trial
# The Smart glue jobs are configured to use 80 DPUs each. As we have an account
# limit of 300 DPUs, we can not run all the smart jobs concurrently. To avoid
# hitting resource limits, we run the inventory staging first, then the daily gas / elec
# reads concurrently, followed by the half-hourly gas / elec reads.
# This also allows us to run R2B withouth being dependent on half-hourly data processing
# completing successfully.
stage_smart_inventory >> stage_daily_gas_reads_asei >> stage_half_hourly_gas_reads_asei >> legacy_asei_half_hourly_smart_ref_jobs_complete
stage_smart_inventory >> stage_daily_elec_reads_asei >> stage_half_hourly_elec_reads_asei >> legacy_asei_half_hourly_smart_ref_jobs_complete

stage_daily_gas_reads_asei >> populate_ref_readings_smart_daily_raw
stage_daily_elec_reads_asei >> populate_ref_readings_smart_daily_raw

populate_ref_readings_smart_daily_raw >> populate_ref_readings_smart_daily >> legacy_asei_daily_smart_ref_jobs_complete
stage_smart_inventory >> populate_ref_smart_inventory_raw >> populate_ref_smart_inventory >> legacy_asei_daily_smart_ref_jobs_complete
legacy_asei_half_hourly_smart_ref_jobs_complete >> refresh_mv_smart_stage2_smarthalfhourlyreads_elec
refresh_mv_smart_stage2_smarthalfhourlyreads_elec >> generate_d0379_task >> copy_d0379_to_sftp_task


# Future Read-to-Bill based on reads from ASe-i & uSmart being pushed in to
# ref_readings_smart_daily_all.
# Note that the dependency on legacy_asei_smart_ref_jobs_complete is somewhat
# arbitrary right now, and will be replaced by a dependency on staging of uSmart
# reads when that is available.
stage_usmart_4_6_1_gas >> crawl_stage2_usmart_4_6_1_gas >> refresh_mv_readings_smart_daily_usmart
stage_usmart_4_6_1_elec >> crawl_stage2_usmart_4_6_1_elec >> refresh_mv_readings_smart_daily_usmart
refresh_mv_readings_smart_daily_usmart >> populate_ref_readings_smart_daily_uSmart_raw
legacy_asei_daily_smart_ref_jobs_complete >> truncate_ref_readings_smart_daily_all
populate_ref_readings_smart_daily_uSmart_raw >> truncate_ref_readings_smart_daily_all
truncate_ref_readings_smart_daily_all >> populate_ref_readings_smart_daily_all_with_usmart_reads >> start_smart_all_billing_reads_jobs
truncate_ref_readings_smart_daily_all >> populate_ref_readings_smart_daily_all_with_asei_elec_reads >> start_smart_all_billing_reads_jobs
truncate_ref_readings_smart_daily_all >> populate_ref_readings_smart_daily_all_with_asei_gas_reads >> start_smart_all_billing_reads_jobs

# Mirror steps
environment = util.get_env()
directory = util.get_dir()
destination_bucket = directory["s3_smart_bucket"]
source_bucket = directory["s3_smart_source_bucket"]
s3_temp_destination_uat_bucket = directory["s3_temp_hh_uat_bucket"]
if environment in ["preprod", "dev"]:

    def create_mirror_task(task_id, path, s3_source_bucket, s3_destination_bucket):
        """
        Wrapper for mirror etl
        """
        return PythonOperator(
            task_id=task_id,
            python_callable=util.process_s3_mirror_job,
            op_kwargs={
                "job_name": task_id,
                "source_input": f"s3://{s3_source_bucket}/stage1/{path}/",
                "destination_input": f"s3://{s3_destination_bucket}/stage1/{path}/",
            },
            dag=dag,
        )

    mirror_asei_smart_inventory = create_mirror_task(
        "mirror_asei_smart_inventory", "Inventory", source_bucket, destination_bucket
    )
    mirror_asei_smartreads_daily_elec = create_mirror_task(
        "mirror_asei_smartreads_daily_elec", "ReadingsSmart/MeterReads/Elec", source_bucket, destination_bucket
    )
    mirror_asei_smartreads_daily_gas = create_mirror_task(
        "mirror_asei_smartreads_daily_gas", "ReadingsSmart/MeterReads/Gas", source_bucket, destination_bucket
    )
    mirror_usmart_smartreads_daily_elec = create_mirror_task(
        "mirror_usmart_smartreads_daily_elec", "uSmart/4.6.1/Elec", source_bucket, destination_bucket
    )
    mirror_usmart_smartreads_daily_gas = create_mirror_task(
        "mirror_usmart_smartreads_daily_gas", "uSmart/4.6.1/Gas", source_bucket, destination_bucket
    )
    mirror_asei_smartreadds_hh_elec = create_mirror_task(
        "mirror_asei_smartreadds_hh_elec", "ReadingsSmart/ProfileData/Elec", source_bucket, destination_bucket
    )
    mirror_asei_smartreadds_hh_gas = create_mirror_task(
        "mirror_asei_smartreadds_hh_gas", "ReadingsSmart/ProfileData/Gas", source_bucket, destination_bucket
    )
    mirror_stage2_usmart_smartreads_hh_elec = create_mirror_task(
        "mirror_stage2_usmart_smartreads_hh_elec",
        "stage2_SmartHalfHourlyReads_Gas",
        source_bucket,
        s3_temp_destination_uat_bucket,
    )
    mirror_stage2_usmart_smartreads_hh_gas = create_mirror_task(
        "mirror_stage2_usmart_smartreads_hh_gas",
        "stage2_SmartHalfHourlyReads_Elec",
        source_bucket,
        s3_temp_destination_uat_bucket,
    )
    # USMART FLOW
    mirror_stage2_usmart_smartreads_hh_elec
    mirror_stage2_usmart_smartreads_hh_gas
    mirror_usmart_smartreads_daily_elec >> stage_usmart_4_6_1_elec
    mirror_usmart_smartreads_daily_gas >> stage_usmart_4_6_1_gas

    # ASEI FLOW
    mirror_asei_smart_inventory >> stage_smart_inventory
    mirror_asei_smartreadds_hh_elec >> stage_half_hourly_elec_reads_asei
    mirror_asei_smartreadds_hh_gas >> stage_half_hourly_gas_reads_asei
    mirror_asei_smartreads_daily_elec >> stage_daily_elec_reads_asei
    mirror_asei_smartreads_daily_gas >> stage_daily_gas_reads_asei

# fmt: on
# pylint: enable=pointless-statement,line-too-long
