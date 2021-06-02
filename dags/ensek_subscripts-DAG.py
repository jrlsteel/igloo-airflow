import sys

sys.path.append("/opt/airflow/enzek-meterpoint-readings")

from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from common.slack_utils import alert_slack
import process_Ensek



args = {
    "owner": "Airflow",
    "start_date": days_ago(2),
    "on_failure_callback": alert_slack,
}


dag = DAG(
    dag_id="test_dag_ensek",
    default_args=args,
    schedule_interval=None,
    tags=["test"],
    catchup=False,
    description="A test Dag for running ensek scripts",
)


process_ensek_transactions = BashOperator(
    task_id="process_ensek_transactions",
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_Ensek/processEnsekTransactions && python process_ensek_transactions.py",
    dag=dag,
)
