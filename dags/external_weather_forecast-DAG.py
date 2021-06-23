import sys
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import sentry_sdk
import datetime

sys.path.append("/opt/airflow/enzek-meterpoint-readings")
import common.process_glue_crawler
import common.utils

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


def fn_weather_forecast_hourly_verify():
    try:
        gsp_weather_station_postcodes = [
            "IP7 7RE",
            "NG16 1JF",
            "TW6 2EQ",
            "LL18 5YD",
            "B46 3JH",
            "NE15 0RH",
            "RH6 0EN",
            "CF62 4JD",
            "BA22 8HT",
            "HU17 7LZ",
            "PA7 5NX",
            "AB21 7DU",
            "CM6 3TH",
            "PE8 6HB",
            "CH4 0GZ",
            "CV23 9EU",
            "NE66 3JF",
            "WA14 3SB",
            "HA4 6NG",
            "CR8 5EG",
            "SP4 0JF",
            "DL7 9NJ",
            "KA9 2PL",
            "IV36 3UH",
        ]
        hourly_forecast_hours = 120
        expected_row_count = len(gsp_weather_station_postcodes) * hourly_forecast_hours

        df = common.utils.execute_query_return_df(
            """select count(*) from ref_weather_forecast_hourly where outcode in ('{}') and forecast_issued = '{}'""".format(
                "', '".join(gsp_weather_station_postcodes), datetime.date.today().isoformat()
            )
        )
        assert df["count"][0] >= expected_row_count
    except Exception as e:
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush(5)
        raise e


def fn_weather_forecast_daily_verify():
    try:
        gsp_weather_station_postcodes = [
            "IP7 7RE",
            "NG16 1JF",
            "TW6 2EQ",
            "LL18 5YD",
            "B46 3JH",
            "NE15 0RH",
            "RH6 0EN",
            "CF62 4JD",
            "BA22 8HT",
            "HU17 7LZ",
            "PA7 5NX",
            "AB21 7DU",
            "CM6 3TH",
            "PE8 6HB",
            "CH4 0GZ",
            "CV23 9EU",
            "NE66 3JF",
            "WA14 3SB",
            "HA4 6NG",
            "CR8 5EG",
            "SP4 0JF",
            "DL7 9NJ",
            "KA9 2PL",
            "IV36 3UH",
        ]
        hourly_forecast_hours = 5
        expected_row_count = len(gsp_weather_station_postcodes) * hourly_forecast_hours

        df = common.utils.execute_query_return_df(
            """select count(*) from ref_weather_forecast_daily where outcode in ('{}') and forecast_issued = '{}'""".format(
                "', '".join(gsp_weather_station_postcodes), datetime.date.today().isoformat()
            )
        )
        assert df["count"][0] >= expected_row_count
    except Exception as e:
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush(5)
        raise e


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
    op_args=["data-crawler-weather-forecast-daily-stage2"],
    python_callable=fn_run_glue_crawler,
    dag=dag,
)

crawler_weather_forcecast_hourly_task = PythonOperator(
    task_id="crawler_weather_forcecast_hourly_task",
    op_args=["data-crawler-weather-forecast-hourly-stage2"],
    python_callable=fn_run_glue_crawler,
    dag=dag,
)

weather_forecast_daily_verify = PythonOperator(
    task_id="weather_forecast_daily_verify_task",
    python_callable=fn_weather_forecast_daily_verify,
    dag=dag,
)

weather_forecast_hourly_verify = PythonOperator(
    task_id="weather_forecast_hourly_verify_task",
    python_callable=fn_weather_forecast_hourly_verify,
    dag=dag,
)

(
    weather_forecast_daily_download
    >> crawler_weather_forcecast_daily_task
    >> weather_forecast_daily_store
    >> weather_forecast_daily_verify
)
(
    weather_forecast_hourly_download
    >> crawler_weather_forcecast_hourly_task
    >> weather_forecast_hourly_store
    >> weather_forecast_hourly_verify
)

#  To avoid overloading the weatherbit.io API, we make sure that the download
#  steps run sequentially.
weather_forecast_daily_download >> weather_forecast_hourly_download
