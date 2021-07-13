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

dag_id = "SMART"


def get_schedule():
    env = config.environment_config["environment"]
    return schedules.get_schedule(env, dag_id)


def sql_wrapper(sql):
    """
    :param: SQL expression to execute
    Executes SQL expression and will pass any errors to Sentry
    """
    try:
        print("Running SQL --- \n   {}".format(sql))
        response = common.utils.execute_sql(sql)
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
)

start_smart_all_mirror_jobs = BashOperator(
    task_id="start_smart_all_mirror_jobs",
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_smart && python start_smart_all_mirror_jobs.py",
    dag=dag,
)

start_smart_all_staging_jobs = BashOperator(
    task_id="start_smart_all_staging_jobs",
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_smart && python start_smart_all_staging_jobs.py",
    dag=dag,
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
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_smart && python process_smart_reads_billing.py",
    dag=dag,
)

refresh_mv_smart_stage2_smarthalfhourlyreads_elec = PythonOperator(
    dag=dag,
    task_id="refresh_mv_smart_stage2_smarthalfhourlyreads_elec",
    python_callable=sql_wrapper,
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
    python_callable=sql_wrapper,
    op_args=["REFRESH MATERIALIZED VIEW mv_readings_smart_daily_usmart"],
    dag=dag,
)

populate_ref_readings_smart_daily_uSmart_raw = PythonOperator(
    task_id="populate_ref_readings_smart_daily_uSmart_raw",
    python_callable=sql_wrapper,
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
    python_callable=sql_wrapper,
    op_args=["""TRUNCATE ref_readings_smart_daily_all"""],
)

populate_ref_readings_smart_daily_all_with_usmart_reads = PythonOperator(
    dag=dag,
    task_id="populate_ref_readings_smart_daily_all_with_usmart_reads",
    python_callable=run_glue_job_await_completion,
    op_args=["_process_ref_tables", "readings_smart_daily_usmart"],
)

populate_ref_readings_smart_daily_all_with_asei_elec_reads = PythonOperator(
    dag=dag,
    task_id="populate_ref_readings_smart_daily_all_with_asei_elec_reads",
    python_callable=run_glue_job_await_completion,
    op_args=["_process_ref_tables", "readings_smart_daily_asei_elec"],
)

populate_ref_readings_smart_daily_all_with_asei_gas_reads = PythonOperator(
    dag=dag,
    task_id="populate_ref_readings_smart_daily_all_with_asei_gas_reads",
    python_callable=run_glue_job_await_completion,
    op_args=["_process_ref_tables", "readings_smart_daily_asei_gas"],
)

legacy_asei_smart_ref_jobs_complete = DummyOperator(dag=dag, task_id="legacy_asei_smart_ref_jobs_complete")

# pylint: disable=pointless-statement,line-too-long
# fmt: off

# Legacy Read-to-Bill & Elective Half-hourly Settlement Trial
start_smart_all_mirror_jobs >> start_smart_all_staging_jobs
start_smart_all_staging_jobs >> populate_ref_readings_smart_daily_raw >> populate_ref_readings_smart_daily >> legacy_asei_smart_ref_jobs_complete
start_smart_all_staging_jobs >> populate_ref_smart_inventory_raw >> populate_ref_smart_inventory >> legacy_asei_smart_ref_jobs_complete
legacy_asei_smart_ref_jobs_complete >> start_smart_all_billing_reads_jobs
legacy_asei_smart_ref_jobs_complete >> refresh_mv_smart_stage2_smarthalfhourlyreads_elec
refresh_mv_smart_stage2_smarthalfhourlyreads_elec >> generate_d0379_task >> copy_d0379_to_sftp_task

# Future Read-to-Bill based on reads from ASe-i & uSmart being pushed in to
# ref_readings_smart_daily_all
start_smart_all_staging_jobs >> crawl_stage2_usmart_4_6_1_gas >> refresh_mv_readings_smart_daily_usmart
start_smart_all_staging_jobs >> crawl_stage2_usmart_4_6_1_elec >> refresh_mv_readings_smart_daily_usmart
refresh_mv_readings_smart_daily_usmart >> populate_ref_readings_smart_daily_uSmart_raw
legacy_asei_smart_ref_jobs_complete >> truncate_ref_readings_smart_daily_all
populate_ref_readings_smart_daily_uSmart_raw >> truncate_ref_readings_smart_daily_all
truncate_ref_readings_smart_daily_all >> populate_ref_readings_smart_daily_all_with_usmart_reads
truncate_ref_readings_smart_daily_all >> populate_ref_readings_smart_daily_all_with_asei_elec_reads
truncate_ref_readings_smart_daily_all >> populate_ref_readings_smart_daily_all_with_asei_gas_reads

# fmt: on
# pylint: enable=pointless-statement,line-too-long
