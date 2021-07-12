import sys

sys.path.append("/opt/airflow/enzek-meterpoint-readings")
from common.slack_utils import alert_slack

import datetime
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import common.process_glue_crawler
import sentry_sdk

crawler_id = "data-crawler-stage2-usmart-4.6.1-elec"

args = {"owner": "Airflow", "start_date": days_ago(2), "on_failure_callback": alert_slack}

dag = DAG(
    dag_id="usmart_stage2_elec_crawler",
    default_args=args,
    schedule_interval=None,
    tags=["cdw"],
    catchup=False,
)


crawler_usmart_stage2_elec_task = PythonOperator(
    task_id="crawler_usmart_stage2_elec_task",
    op_args=["data-crawler-usmart-stage2-elec"],
    python_callable=common.process_glue_crawler.run_glue_crawler,
    dag=dag,
)
