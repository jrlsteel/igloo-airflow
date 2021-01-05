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
    dag_id='table_comparisons_cdb_tables_old_prod_old_preprod',
    default_args=args,
    schedule_interval='00 09 * * *',
    tags=['cdw'],
    # We only want to run one at a time to conserve resource, but
    # we want all to run, even if any fail.
    concurrency=1
)

task_ids = [
'ref_cdb_addresses',
'ref_cdb_adequacy_adjustmentactions',
'ref_cdb_adequacy_adjustments',
'ref_cdb_adequacy_tasks',
'ref_cdb_attribute_types',
'ref_cdb_attribute_values',
'ref_cdb_attributes',
'ref_cdb_broker_mappings',
'ref_cdb_broker_maps',
'ref_cdb_broker_signups',
'ref_cdb_quotes',
'ref_cdb_raf_interest',
'ref_cdb_raf_referrals',
'ref_cdb_registrations',
'ref_cdb_reward_accounts',
'ref_cdb_reward_transaction_postings',
'ref_cdb_reward_transactions',
'ref_cdb_supply_contracts',
'ref_cdb_survey_category',
'ref_cdb_survey_questions',
'ref_cdb_survey_response',
'ref_cdb_surveys',
'ref_cdb_user_permissions',
'ref_cdb_users',
'ref_cdb_users_audit',
'ref_cdb_winter_uplifts',
]

d1 = DummyOperator(task_id='Start', dag=dag)

for task_id in task_ids:
    operator = BashOperator(
        task_id=task_id,
        bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_table_comparisons && python compare_tables.py --table-comparison-config old_prod_old_preprod_{} --output-to-s3'.format(task_id),
        dag=dag
    )
    d1 >> operator

