from __future__ import print_function

import time
from builtins import range
from pprint import pprint

from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import PythonVirtualenvOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator


args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='table_comparisons_ensek_tables_old_prod_new_prod',
    default_args=args,
    schedule_interval='00 17 * * *',
    tags=['cdw'],
    # We only want to run one at a time to conserve resource, but
    # we want all to run, even if any fail.
    concurrency=1,
    catchup=False,
    max_active_runs=1,
)

task_ids = [
    'ref_account_transactions',
    'ref_meterpoints',
    'ref_meterpoints_attributes',
    'ref_meterpoints_attributes_raw',
    'ref_meterpoints_raw',
    'ref_meters',
    'ref_meters_attributes',
    'ref_meters_attributes_raw',
    'ref_meters_raw',
    'ref_nrl',
    'ref_occupier_accounts',
    'ref_occupier_accounts_raw',
    'ref_readings_internal',
    'ref_readings_internal_nosi',
    'ref_readings_internal_nrl',
    'ref_readings_internal_valid',
    'ref_registers',
    'ref_registers_attributes',
    'ref_registers_attributes_raw',
    'ref_registers_raw',
    'ref_registrations_meterpoints_status_elec',
    'ref_registrations_meterpoints_status_gas',
    'ref_tariff_history',
    'ref_tariff_history_elec_sc',
    'ref_tariff_history_elec_ur',
    'ref_tariff_history_gas_sc',
    'ref_tariff_history_gas_ur',
    'ref_tariff_history_generated',
    'ref_tariffs',
]

d1 = DummyOperator(task_id='Start', dag=dag)

for task_id in task_ids:
    operator = BashOperator(
        task_id=task_id,
        bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_table_comparisons && python compare_tables.py --table-comparison-config old_prod_new_prod_{} --output-to-s3'.format(task_id),
        dag=dag
    )
    d1 >> operator
