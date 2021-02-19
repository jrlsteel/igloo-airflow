import sys
sys.path.append("/opt/airflow/enzek-meterpoint-readings")

import datetime
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from process_smart.d0379 import generate_d0379, copy_d0379_to_sftp

args = {
    "owner": "Airflow",
    "start_date": days_ago(2),
}

dag = DAG(
    dag_id="igloo_smart_d0379",
    default_args=args,
    schedule_interval=None,
    tags=["cdw"],
    catchup=False,
)

def generate_d0379_wrapper(execution_date):
    """
    :param: execution_date a string in the form 'YYYY-MM-DD'
    """
    print("D0379 execution_date={}".format(execution_date))
    d0379_date = datetime.date.today() - datetime.timedelta(days=1)
    generate_d0379(d0379_date)

def copy_d0379_to_sftp_wrapper(execution_date):
    """
    :param: execution_date a string in the form 'YYYY-MM-DD'
    """
    print("copy_d0379_to_sftp execution_date={}".format(execution_date))
    d0379_date = datetime.date.today() - datetime.timedelta(days=1)
    copy_d0379_to_sftp(d0379_date)

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

generate_d0379_task >> copy_d0379_to_sftp_task
