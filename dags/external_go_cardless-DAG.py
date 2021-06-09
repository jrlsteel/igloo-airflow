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


from process_verification.verification_template import (
    verify_number_of_rows_in_table,
)

sys.path.append("/opt/airflow/enzek-meterpoint-readings")

from common.slack_utils import alert_slack

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2), # don't know what this is doing
    'on_failure_callback': alert_slack
}

dag = DAG(
    dag_id='go_cardless',
    default_args=args,
    schedule_interval='00 02 * * *',
    tags=['cdw'],
    catchup=False,
    max_active_runs=1,
)

start_go_cardless_api_extracts = BashOperator(
    task_id='start_go_cardless_api_extracts',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_go_cardless && python start_go_cardless_api_extracts.py',
    dag=dag,
)


verify_mandate_updates_table_empty = PythonOperator(
    task_id="verify_mandate_update_table_empty",
    python_callable=verify_number_of_rows_in_table,
    op_kwargs={
        "table_name": "vw_gc_updates_mandates",
        "expected_count": 0,
    },
    dag=dag,
)
verify_mandate_updates_table_empty.doc = """*Purpose*: Verify all mandates have been loaded into Redshift correctly
    *Suggest action on failure*: Investigate why mandates have not been ingested into Redshift
    *Justification*: The vw_gc_updates_mandates table should be empty as it displays mandates that are in S3 but not the ref tables.
    """

verify_subscription_updates_table_empty = PythonOperator(
    task_id="verify_subscription_updates_table_empty",
    python_callable=verify_number_of_rows_in_table,
    op_kwargs={
        "table_name": "vw_gc_updates_subscriptions",
        "expected_count": 0,
    },
    dag=dag,
)
verify_subscription_updates_table_empty.doc = """*Purpose*: Verify all subscriptions have been loaded into Redshift correctly
    *Suggest action on failure*: Investigate why subscriptions have not been ingested into Redshift
    *Justification*: The vw_gc_updates_subscriptions table should be empty as it displays subscriptions that are in S3 but not the ref tables.
    """

verify_payment_updates_table_empty = PythonOperator(
    task_id="verify_payment_updates_table_empty",
    python_callable=verify_number_of_rows_in_table,
    op_kwargs={
        "table_name": "vw_gc_updates_payments",
        "expected_count": 0,
    },
    dag=dag,
)
verify_payment_updates_table_empty.doc = """*Purpose*: Verify all payments have been loaded into Redshift correctly
    *Suggest action on failure*: Investigate why payments have not been ingested into Redshift
    *Justification*: The vw_gc_updates_payments table should be empty as it displays payments that are in S3 but not the ref tables.
    """

verify_refund_updates_table_empty = PythonOperator(
    task_id="verify_refund_updates_table_empty",
    python_callable=verify_number_of_rows_in_table,
    op_kwargs={
        "table_name": "vw_gc_updates_refunds",
        "expected_count": 0,
    },
    dag=dag,
)
verify_refund_updates_table_empty.doc = """*Purpose*: Verify all refunds have been loaded into Redshift correctly
    *Suggest action on failure*: Investigate why refunds have not been ingested into Redshift
    *Justification*: The vw_gc_updates_refunds table should be empty as it displays refunds that are in S3 but not the ref tables.
    """


start_go_cardless_api_extracts >> verify_mandate_updates_table_empty
start_go_cardless_api_extracts >> verify_subscription_updates_table_empty
start_go_cardless_api_extracts >> verify_payment_updates_table_empty
start_go_cardless_api_extracts >> verify_refund_updates_table_empty
