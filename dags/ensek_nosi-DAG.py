import sys

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

sys.path.append("/opt/airflow/enzek-meterpoint-readings")

from conf import config
from common import schedules
from common.slack_utils import alert_slack

dag_id = "ensek_nosi"


def get_schedule():
    env = config.environment_config["environment"]
    return schedules.get_schedule(env, dag_id)


args = {
    "owner": "Airflow",
    "start_date": days_ago(2),  # don't know what this is doing
    "on_failure_callback": alert_slack,
}

dag = DAG(
    dag_id=dag_id,
    default_args=args,
    schedule_interval=get_schedule(),
    tags=["cdw"],
    catchup=False,
    max_active_runs=1,
)

mirror_nosi = BashOperator(
    task_id="mirror_nosi",
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_Nosi && python start_ensek_readings_nosi_mirror_only_jobs.py",
    dag=dag,
)

start_ensek_readings_nosi_staging_jobs = BashOperator(
    task_id="start_ensek_readings_nosi_staging_jobs",
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_Nosi && python start_ensek_readings_nosi_staging_jobs.py",
    dag=dag,
)

start_ensek_readings_nosi_ref_jobs = BashOperator(
    task_id="start_ensek_readings_nosi_ref_jobs",
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_Nosi && python start_ensek_readings_nosi_ref_jobs.py",
    dag=dag,
)

mirror_nosi >> start_ensek_readings_nosi_staging_jobs >> start_ensek_readings_nosi_ref_jobs
