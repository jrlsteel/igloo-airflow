from __future__ import print_function

import sys

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

sys.path.append("/opt/airflow/enzek-meterpoint-readings")

from common.slack_utils import alert_slack

args = {
    "owner": "Airflow",
    "start_date": days_ago(2),  # don't know what this is doing
    "on_failure_callback": alert_slack,
}

dag = DAG(
    dag_id="ensek_pa_no_glue",
    default_args=args,
    schedule_interval=None,
    tags=["cdw"],
    catchup=False,
    max_active_runs=1,
)


process_ensek_meterpoints_no_history = BashOperator(
    task_id="process_ensek_meterpoints_no_history",
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_Ensek && python processEnsekMeterpoints/process_ensek_meterpoints_no_history.py",
    dag=dag,
)

process_ensek_internal_readings = BashOperator(
    task_id="process_ensek_internal_readings",
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_Ensek && python processEnsekReadings/process_ensek_internal_readings.py",
    dag=dag,
)


process_ensek_meterpoints_no_history >> process_ensek_internal_readings

# process_ensek_meterpoints_no_history >> process_ensek_internal_readings >> start_ensek_pa_staging_jobs >> start_ensek_pa_ref_jobs
