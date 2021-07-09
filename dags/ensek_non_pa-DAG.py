import sys
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

sys.path.append("/opt/airflow/enzek-meterpoint-readings")

from conf import config
from common import schedules
from common.slack_utils import alert_slack

dag_id = "ensek_non_pa"


def get_schedule():
    env = config.environment_config["environment"]
    return schedules.get_schedule(env, dag_id)


args = {
    "owner": "Airflow",
    "start_date": days_ago(2),
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


def processEnsekBashOperator(script_name):
    return BashOperator(
        task_id=script_name,
        bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_Ensek && python {}.py".format(script_name),
        dag=dag,
    )


start_ensek_api_mirror_only_jobs = processEnsekBashOperator("start_ensek_api_mirror_only_jobs")

start_ensek_non_pa_staging_jobs = processEnsekBashOperator("start_ensek_non_pa_staging_jobs")

start_ensek_non_pa_ref_jobs = processEnsekBashOperator("start_ensek_non_pa_ref_jobs")

igloo_calculated_trigger = TriggerDagRunOperator(
    task_id="igloo_calculated_trigger", trigger_dag_id="igloo_calculated", dag=dag
)

igloo_alp_trigger = TriggerDagRunOperator(task_id="igloo_alp_trigger", trigger_dag_id="igloo_alp", dag=dag)

(
        start_ensek_api_mirror_only_jobs
        >> start_ensek_non_pa_staging_jobs
        >> start_ensek_non_pa_ref_jobs
        >> igloo_calculated_trigger
)
