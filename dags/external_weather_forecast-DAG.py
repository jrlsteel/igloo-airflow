import sys
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import sentry_sdk

sys.path.append("/opt/airflow/enzek-meterpoint-readings")
import common.process_glue_crawler

from conf import config
from common import schedules
from common.slack_utils import alert_slack

dag_id = "weather_forecasts"


def get_schedule():
    env = config.environment_config["environment"]
    return schedules.get_schedule(env, dag_id)

def fn_run_glue_crawler(crawler_id):
    """
    :param: Crawler Name'
    """
    try:
        print("Running Weather Forecast Crawler Daily={}".format(crawler_id))
        common.process_glue_crawler.run_glue_crawler(crawler_id)
    except Exception as e:
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush(5)
        raise e


args = {
    'owner': 'Airflow',
    'start_date': days_ago(2), # don't know what this is doing
    'on_failure_callback': alert_slack
}

dag = DAG(
    dag_id=dag_id,
    default_args=args,
    schedule_interval=get_schedule(),
    tags=["cdw"],
    catchup=False,
    max_active_runs=1,
)

weather_forecast_daily_download = BashOperator(
    task_id="weather_forecast_daily_download",
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_WeatherData && python start_forecast_weather_jobs.py --download-daily",
    dag=dag,
)

weather_forecast_daily_store = BashOperator(
    task_id="weather_forecast_daily_store",
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_WeatherData && python start_forecast_weather_jobs.py --store-daily",
    dag=dag,
)

weather_forecast_hourly_download = BashOperator(
    task_id="weather_forecast_hourly_download",
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_WeatherData && python start_forecast_weather_jobs.py --download-hourly",
    dag=dag,
)

weather_forecast_hourly_store = BashOperator(
    task_id="weather_forecast_hourly_store",
    bash_command="cd /opt/airflow/enzek-meterpoint-readings/process_WeatherData && python start_forecast_weather_jobs.py --store-hourly",
    dag=dag,
)

crawler_weather_forcecast_daily_task = PythonOperator(
    task_id="crawler_weather_forcecast_daily_task",
    op_args=['data-crawler-weather-forecast-daily-stage2'],
    python_callable=fn_run_glue_crawler,
    dag=dag,
)

crawler_weather_forcecast_hourly_task = PythonOperator(
    task_id="crawler_weather_forcecast_hourly_task",
    op_args=['data-crawler-weather-forecast-hourly-stage2'],
    python_callable=fn_run_glue_crawler,
    dag=dag,
)

weather_forecast_daily_download >> weather_forecast_daily_store >> crawler_weather_forcecast_daily_task 
weather_forecast_hourly_download >> weather_forecast_hourly_store >> crawler_weather_forcecast_hourly_task

#  To avoid overloading the weatherbit.io API, we make sure that the download
#  steps run sequentially.
weather_forecast_daily_download >> weather_forecast_hourly_download
