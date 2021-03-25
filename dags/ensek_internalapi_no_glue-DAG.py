from __future__ import print_function

import time
from builtins import range
from pprint import pprint

from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import PythonVirtualenvOperator
from airflow.operators.bash_operator import BashOperator

sys.path.append("/opt/airflow/enzek-meterpoint-readings")

from common.slack_utils import alert_slack


args = {
    'owner': 'Airflow',
    'start_date': days_ago(2), # don't know what this is doing
    'on_failure_callback': alert_slack
}

dag = DAG(
    dag_id='ensek_internal_apis_no_glue',
    default_args=args,
    schedule_interval=None,
    tags=['cdw'],
    catchup=False,
    max_active_runs=1,
)



process_ensek_internal_estimates = BashOperator(
    task_id='process_ensek_internal_estimates',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_Ensek && python processEnsekEstimates/process_ensek_internal_estimates.py',
    dag=dag,
)

process_ensek_internal_readings = BashOperator(
    task_id='process_ensek_internal_readings',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_Ensek && python processEnsekReadings/process_ensek_internal_readings.py',
    dag=dag,
)


process_ensek_internal_estimates >> process_ensek_internal_readings
