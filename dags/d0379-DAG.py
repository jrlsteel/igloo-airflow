import sys
sys.path.append("/opt/airflow/enzek-meterpoint-readings")

import datetime
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from process_smart.d0379 import generate_d0379

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
    print("D0379 execution_date={}".format(execution_date))
    d0379_date = datetime.date.today() - datetime.timedelta(days=1)
    generate_d0379(d0379_date)

d0379 = PythonOperator(
    task_id="generate_d0379",
    python_callable=generate_d0379_wrapper,
    op_args=["{{ ds }}"],
    dag=dag,
)


d0379
