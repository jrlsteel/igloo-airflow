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
    dag_id='igloo_alp',
    default_args=args,
    schedule_interval=None,
    tags=['cdw']
)

process_ALP_DAF = BashOperator(
    task_id='process_ALP_DAF',
    bash_command='cd /opt/code/enzek-meterpoint-readings/process_ALP && ../.venv/bin/python process_ALP_DAF.py',
    dag=dag,
)

process_ALP_PN = BashOperator(
    task_id='process_ALP_PN',
    bash_command='cd /opt/code/enzek-meterpoint-readings/process_ALP && ../.venv/bin/python process_ALP_PN.py',
    dag=dag,
)

processALP_CV = BashOperator(
    task_id='processALP_CV',
    bash_command='cd /opt/code/enzek-meterpoint-readings/process_ALP && ../.venv/bin/python processALP_CV.py',
    dag=dag,
)

processALP_WCF = BashOperator(
    task_id='processALP_WCF',
    bash_command='cd /opt/code/enzek-meterpoint-readings/process_ALP && ../.venv/bin/python processALP_WCF.py',
    dag=dag,
)

start_alp_historical_staging_jobs = BashOperator(
    task_id='start_alp_historical_staging_jobs',
    bash_command='cd /opt/code/enzek-meterpoint-readings/process_ALP && ../.venv/bin/python start_alp_historical_staging_jobs.py',
    dag=dag,
)

start_alp_historical_ref_jobs = BashOperator(
    task_id='start_alp_historical_ref_jobs',
    bash_command='cd /opt/code/enzek-meterpoint-readings/process_ALP && ../.venv/bin/python start_alp_historical_ref_jobs.py',
    dag=dag,
)

start_alp_historical_calc_jobs = BashOperator(
    task_id='start_alp_historical_calc_jobs',
    bash_command='cd /opt/code/enzek-meterpoint-readings/process_ALP && ../.venv/bin/python start_alp_historical_calc_jobs.py',
    dag=dag,
)

process_ALP_DAF >> processALP_CV >> process_ALP_PN >> processALP_WCF >> start_alp_historical_staging_jobs >> start_alp_historical_ref_jobs >> start_alp_historical_calc_jobs



