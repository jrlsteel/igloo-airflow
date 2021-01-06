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
    dag_id='table_comparisons_other_tables_old_prod_new_prod',
    default_args=args,
    schedule_interval='00 20 * * *',
    tags=['cdw'],
    # We only want to run one at a time to conserve resource, but
    # we want all to run, even if any fail.
    concurrency=1,
    catchup=False,
)

task_ids = [
    'ref_alp_igloo_cv',
    'ref_alp_igloo_daf_wcf',
    'ref_cumulative_alp_cv',
    'ref_cumulative_ppc',
    'ref_d18_bpp',
    'ref_d18_bpp_forecast',
    'ref_d18_igloo_bpp',
    'ref_d18_igloo_bpp_forecast',
    'ref_d18_igloo_ppc',
    'ref_d18_igloo_ppc_forecast',
    'ref_d18_ppc',
    'ref_d18_ppc_forecast',
    'ref_epc_certificates_all',
    'ref_epc_recommendations_all',
    'ref_historical_weather',
    'ref_land_registry',
]

d1 = DummyOperator(task_id='Start', dag=dag)

for task_id in task_ids:
    operator = BashOperator(
        task_id=task_id,
        bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_table_comparisons && python compare_tables.py --table-comparison-config old_prod_new_prod_{} --output-to-s3'.format(task_id),
        dag=dag
    )
    d1 >> operator

