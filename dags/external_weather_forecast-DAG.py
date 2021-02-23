import sys
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

sys.path.append("/opt/airflow/enzek-meterpoint-readings")

from conf import config
from common import schedules

dag_id = "weather_forecasts"


def get_schedule():
    env = config.environment_config["environment"]
    return schedules.get_schedule(env, dag_id)


args = {
    "owner": "Airflow",
    "start_date": days_ago(2),  #  don't know what this is doing
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

weather_forecast_daily_download >> weather_forecast_daily_store
weather_forecast_hourly_download >> weather_forecast_hourly_store

#  To avoid overloading the weatherbit.io API, we make sure that the download
#  steps run sequentially.
weather_forecast_daily_download >> weather_forecast_hourly_download
