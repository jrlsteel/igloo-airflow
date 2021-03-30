import sys
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

sys.path.append('/opt/airflow/enzek-meterpoint-readings')

from conf import config
from common import schedules
from common.slack_utils import alert_slack

dag_id = 'ensek_pa'

def get_schedule():
    env = config.environment_config['environment']
    return schedules.get_schedule(env, dag_id)

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2), # don't know what this is doing
    'on_failure_callback': alert_slack
}

dag = DAG(
    dag_id=dag_id,
    default_args=args,
    schedule_interval=get_schedule(),
    tags=['cdw'],
    catchup=False,
    max_active_runs=1,
)

process_customerdb = BashOperator(
    task_id='process_customerdb',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_Ensek && python start_customerdb_jobs.py',
    dag=dag,
)

start_ensek_api_pa_mirror_only_jobs = BashOperator(
    task_id='start_ensek_api_pa_mirror_only_jobs',
    bash_command='cd /opt/airflow/enzek-meterpoint-readings/process_Ensek && python start_ensek_api_pa_mirror_only_jobs.py',
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

process_customerdb >> start_ensek_api_pa_mirror_only_jobs >> start_ensek_pa_staging_jobs >> start_ensek_pa_ref_jobs
