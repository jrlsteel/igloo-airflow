from cdw.common.slack_utils import alert_slack

from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import cdw.common.process_glue_crawler

crawler_id = "data-crawler-weather-forecast-daily-stage2"

args = {"owner": "Airflow", "start_date": days_ago(2), "on_failure_callback": alert_slack}

dag = DAG(
    dag_id="weather_forecasts_crawler_daily",
    default_args=args,
    schedule_interval=None,
    tags=["cdw"],
    catchup=False,
)


def fn_run_glue_crawler(crawler_id):
    """
    :param: Crawler Name'
    """

    # Slack logging moved to this common module
    print("Running Weather Forecast Crawler Daily={}".format(crawler_id))
    cdw.common.process_glue_crawler.run_glue_crawler(crawler_id)


crawler_weather_forcecast_daily_task = PythonOperator(
    task_id="crawler_weather_forcecast_daily_task",
    op_args=["data-crawler-weather-forecast-daily-stage2"],
    python_callable=fn_run_glue_crawler,
    dag=dag,
)

crawler_weather_forcecast_daily_task
