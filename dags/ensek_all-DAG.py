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
    dag_id='ensek_all',
    default_args=args,
    schedule_interval=None,
    tags=['cdw']
)
#
# process_customerdb = BashOperator(
#     task_id='process_customerdb',
#     bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_Ensek && python start_customerdb_jobs.py',
#     dag=dag,
# )


process_ensek_meterpoints_no_history = BashOperator(
    task_id='process_ensek_meterpoints_no_history',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_Ensek && python processEnsekMeterpoints/process_ensek_meterpoints_no_history.py',
    dag=dag,
)

process_ensek_registration_meterpoint_status = BashOperator(
    task_id='process_ensek_registration_meterpoint_status.py',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_Ensek && python processEnsekStatus/process_ensek_registration_meterpoint_status.py',
    dag=dag,
)

process_ensek_internal_estimates = BashOperator(
    task_id='process_ensek_internal_estimates',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_Ensek && python processEnsekEstimates/process_ensek_internal_estimates.py',
    dag=dag,
)

process_ensek_tariffs_history = BashOperator(
    task_id='process_ensek_tariffs_history',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_Ensek && python processEnsekTariffs/process_ensek_tariffs_history.py',
    dag=dag,
)

process_ensek_internal_readings = BashOperator(
    task_id='process_ensek_internal_readings',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_Ensek && python processEnsekReadings/process_ensek_internal_readings.py',
    dag=dag,
)


process_ensek_account_settings = BashOperator(
    task_id='process_ensek_account_settings',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_Ensek && python processEnsekAccountSettings/process_ensek_account_settings.py',
    dag=dag,
)

process_ensek_transactions = BashOperator(
    task_id='process_ensek_transactions',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_Ensek && python processEnsekTransactions/process_ensek_transactions.py',
    dag=dag,
)

start_ensek_pa_staging_jobs = BashOperator(
    task_id='start_ensek_pa_staging_jobs',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_Ensek && python start_ensek_pa_staging_jobs.py',
    dag=dag,
)

start_ensek_pa_ref_jobs = BashOperator(
    task_id='start_ensek_pa_ref_jobs',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_Ensek && python start_ensek_pa_ref_jobs.py',
    dag=dag,
)

start_ensek_non_pa_staging_jobs = BashOperator(
    task_id='start_ensek_non_pa_staging_jobs',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_Ensek && python start_ensek_non_pa_staging_jobs.py',
    dag=dag,
)

start_ensek_non_pa_ref_jobs = BashOperator(
    task_id='start_ensek_non_pa_ref_jobs',
    bash_command='cd /opt/code/enzek-meterpoint-readings/process_Ensek && python start_ensek_non_pa_ref_jobs.py',
    dag=dag,
)

#process_customerdb >> process_ensek_meterpoints_no_history >> process_ensek_registration_meterpoint_status >> process_ensek_tariffs_history >> process_ensek_account_settings >> process_ensek_transactions >> start_ensek_pa_staging_jobs >> start_ensek_pa_ref_jobs >> start_ensek_non_pa_staging_jobs >> start_ensek_non_pa_ref_jobs

process_ensek_meterpoints_no_history >> process_ensek_registration_meterpoint_status >> process_ensek_tariffs_history >> process_ensek_account_settings >> process_ensek_transactions

process_ensek_internal_readings >> process_ensek_internal_estimates

process_ensek_transactions >> process_ensek_internal_estimates >> start_ensek_pa_staging_jobs >> start_ensek_pa_ref_jobs >> start_ensek_non_pa_staging_jobs >> start_ensek_non_pa_ref_jobs
