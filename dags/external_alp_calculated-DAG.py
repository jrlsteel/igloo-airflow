from __future__ import print_function

import time
from builtins import range
from pprint import pprint

from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import PythonVirtualenvOperator
from airflow.operators.bash_operator import BashOperator


args = {
    'owner': 'Airflow',
    'start_date': days_ago(2), # don't know what this is doing
}

dag = DAG(
    dag_id='igloo_alp_calculated',
    default_args=args,
    schedule_interval='00 09 * * *',
    tags=['cdw'],
    catchup=False,
)

processALP_CV = BashOperator(
    task_id='processALP_CV',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_ALP && python processALP_CV.py',
    dag=dag,
)

processALP_WCF = BashOperator(
    task_id='processALP_WCF',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_ALP && python processALP_WCF.py',
    dag=dag,
)

start_alp_historical_staging_jobs = BashOperator(
    task_id='start_alp_historical_staging_jobs',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_ALP && python start_alp_historical_staging_jobs.py',
    dag=dag,
)

start_alp_historical_ref_jobs = BashOperator(
    task_id='start_alp_historical_ref_jobs',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_ALP && python start_alp_historical_ref_jobs.py',
    dag=dag,
)

start_alp_historical_calc_jobs = BashOperator(
    task_id='start_alp_historical_calc_jobs',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_ALP && python start_alp_historical_calc_jobs.py',
    dag=dag,
)

processALP_CV >> processALP_WCF >> start_alp_historical_staging_jobs >> start_alp_historical_ref_jobs >> start_alp_historical_calc_jobs



