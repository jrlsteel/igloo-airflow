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


args = {
    "owner": "Airflow",
    "start_date": days_ago(2),  #  don't know what this is doing
}

dag = DAG(
    dag_id="igloo_smart_all",
    default_args=args,
    schedule_interval="45 09 * * *",
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

start_smart_all_mirror_jobs >> start_smart_all_staging_jobs >> start_smart_all_ref_jobs
start_smart_all_ref_jobs >> start_smart_all_billing_reads_jobs
start_smart_all_ref_jobs >> smart_all_refresh_mv_hh_elec_reads_jobs >> generate_d0379_task >> copy_d0379_to_sftp_task
