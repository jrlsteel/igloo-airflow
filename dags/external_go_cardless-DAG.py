import sys

sys.path.append("/opt/airflow/enzek-meterpoint-readings")

from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


from process_verification.verification_template import (
    verify_number_of_rows_in_table,
    verify_table_column_value_greater_than,
)

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


verify_events_table_has_events_in_last_24hrs = PythonOperator(
    task_id="verify_events_table_has_events_in_last_24hrs",
    python_callable=verify_table_column_value_greater_than,
    op_kwargs={
        "table_name": "aws_fin_stage1_extracts.fin_go_cardless_api_events",
        "column_name": "max(created_at)",
        "comparison_value": (days_ago(1).strftime('%Y-%m-%dT%H:%M:%S.%f'))[:-3] + 'Z',
    },
    dag=dag,
)
verify_events_table_has_events_in_last_24hrs.doc = """*Purpose*: Verify that we have an entry in the events table with a created_at greater than the timestamp 24 hours ago.
    *Suggest action on failure*: Investigate why we have not received an event in the last 24 hours
    *Justification*: We would expect to receive at least one event within the last 24 hours from GoCardless if the DAG is successful
    """


start_go_cardless_api_extracts >> verify_events_table_has_events_in_last_24hrs
start_go_cardless_api_extracts >> verify_mandate_updates_table_empty
start_go_cardless_api_extracts >> verify_subscription_updates_table_empty
start_go_cardless_api_extracts >> verify_payment_updates_table_empty
start_go_cardless_api_extracts >> verify_refund_updates_table_empty
