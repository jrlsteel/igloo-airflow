from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import sys
import datetime
import sentry_sdk

sys.path.append("/opt/airflow/enzek-meterpoint-readings")
from process_smart.start_smart_refresh_mv_hh import refresh_mv_hh_elec_reads
from process_smart.d0379 import generate_d0379, copy_d0379_to_sftp
from common.slack_utils import alert_slack
import common
from common.process_glue_job import run_glue_job_await_completion
from common.process_glue_crawler import run_glue_crawler

args = {
    "owner": "Airflow",
    "start_date": days_ago(2),  # don't know what this is doing
    "on_failure_callback": alert_slack,
}

dag = DAG(
    dag_id="igloo_smart_all",
    default_args=args,
    schedule_interval="45 09 * * *",
    tags=["cdw"],
    catchup=False,
    max_active_runs=1,
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
    doc_md=""" *Purpose* Runs Glue Job for data described in task_id.
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

start_smart_all_mirror_jobs = BashOperator(
    task_id="start_smart_all_mirror_jobs",
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_smart && python start_smart_all_mirror_jobs.py",
    dag=dag,
)

start_smart_all_ref_jobs = BashOperator(
    task_id="start_smart_all_ref_jobs",
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_smart && python start_smart_all_reporting_jobs.py",
    dag=dag,
)

start_smart_all_billing_reads_jobs = BashOperator(
    task_id="start_smart_all_billing_reads_jobs",
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_smart && python process_smart_reads_billing.py",
    dag=dag,
)

smart_all_refresh_mv_hh_elec_reads_jobs = PythonOperator(
    dag=dag,
    task_id="smart_all_refresh_mv_hh_elec_reads_jobs_task",
    python_callable=refresh_mv_hh_elec_reads,
)


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


def sql_wrapper(sql):
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


# pylint: disable=pointless-statement,line-too-long
# fmt: off

# Legacy Read-to-Bill & Half-hourly Settlement Trial
start_smart_all_mirror_jobs >> stage_smart_inventory
# The Smart glue jobs are configured to use 80 DPUs each. As we have an account
# limit of 300 DPUs, we can not run all the smart jobs concurrently. To avoid
# hitting resource limits, we run the inventory staging first, then the daily gas / elec
# reads concurrently, followed by the half-hourly gas / elec reads.
# This also allows us to run R2B withouth being dependent on half-hourly data processing
# completing successfully.
stage_smart_inventory >> stage_daily_gas_reads_asei >> stage_half_hourly_gas_reads_asei >> start_smart_all_ref_jobs
stage_smart_inventory >> stage_daily_elec_reads_asei >> stage_half_hourly_elec_reads_asei >> start_smart_all_ref_jobs

start_smart_all_ref_jobs >> start_smart_all_billing_reads_jobs
start_smart_all_ref_jobs >> smart_all_refresh_mv_hh_elec_reads_jobs
smart_all_refresh_mv_hh_elec_reads_jobs >> generate_d0379_task >> copy_d0379_to_sftp_task

# fmt: on
# pylint: enable=pointless-statement,line-too-long
