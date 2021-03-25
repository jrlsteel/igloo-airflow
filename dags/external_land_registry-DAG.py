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
    dag_id='igloo_land_registry',
    default_args=args,
    schedule_interval='00 16 * * *',
    tags=['cdw'],
    catchup=False,
    max_active_runs=1,
)

process_land_registry = BashOperator(
    task_id='process_land_registry',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_LandRegistry && python process_land_registry.py',
    dag=dag,
)

start_land_registry_staging_jobs = BashOperator(
    task_id='start_land_registry_staging_jobs',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_LandRegistry && python start_land_registry_staging_jobs.py',
    dag=dag,
)

start_land_registry_ref_jobs = BashOperator(
    task_id='start_land_registry_ref_jobs',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_LandRegistry && python start_land_registry_ref_jobs.py',
    dag=dag,
)

process_land_registry >> start_land_registry_staging_jobs >> start_land_registry_ref_jobs
