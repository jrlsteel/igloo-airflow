from __future__ import print_function

import time
import sys
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
    "owner": "Airflow",
    "start_date": days_ago(2),
    "on_failure_callback": alert_slack,
}

dag = DAG(
    dag_id="igloo_alp",
    default_args=args,
    schedule_interval=None,
    tags=["cdw"],
    catchup=False,
    max_active_runs=1,
)

processALP_CV = BashOperator(
    task_id="processALP_CV",
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_ALP && python processALP_CV.py",
    dag=dag,
)

processALP_WCF = BashOperator(
    task_id="processALP_WCF",
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_ALP && python processALP_WCF.py",
    dag=dag,
)

start_alp_historical_staging_jobs = BashOperator(
    task_id="start_alp_historical_staging_jobs",
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_ALP && python start_alp_historical_staging_jobs.py",
    dag=dag,
)

start_alp_historical_ref_jobs = BashOperator(
    task_id="start_alp_historical_ref_jobs",
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_ALP && python start_alp_historical_ref_jobs.py",
    dag=dag,
)

(processALP_CV >> processALP_WCF >> start_alp_historical_staging_jobs >> start_alp_historical_ref_jobs)
