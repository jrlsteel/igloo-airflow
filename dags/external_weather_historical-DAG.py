from __future__ import print_function
from datetime import datetime, timedelta

from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import PythonVirtualenvOperator
from airflow.operators.bash_operator import BashOperator

import sentry_sdk

from cdw.common.process_glue_job import run_glue_job_await_completion
from cdw.common import schedules
import cdw.common.process_glue_crawler
import cdw.common.utils
from cdw.common import utils as util
from cdw.common.slack_utils import alert_slack

environment = cdw.common.utils.get_env()

dag_id = "igloo_weather_historical"

task_slack_report_string = """
*Purpose*: {0}
*Failure Remediation*: {1}
*Justification*: {2}
"""


def get_schedule():
    return schedules.get_schedule(environment, dag_id)


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


def fn_weather_historical_daily_verify():
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
        hourly_historical_hours = 24
        expected_row_count = len(gsp_weather_station_postcodes) * hourly_historical_hours
        historical_date = datetime.now() - timedelta(1)
        historical_date = datetime.strftime(historical_date, "%Y-%m-%d")

        df = cdw.common.utils.execute_query_return_df(
            """select count(*) from ref_historical_weather where postcode in ('{}') and trunc(timestamp_utc) = '{}'""".format(
                "', '".join(gsp_weather_station_postcodes), historical_date
            )
        )
        assert df["count"][0] >= expected_row_count
    except Exception as e:
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush(5)
        raise e


fetch_stage1_data = BashOperator(
    task_id="fetch_stage1_data",
    bash_command="cd /opt/airflow/cdw/process_WeatherData && python processHistoricalWeatherData.py",
    dag=dag,
)

weather_staging_job = PythonOperator(
    task_id="weather_staging_job",
    python_callable=run_glue_job_await_completion,
    op_args=["process_staging_files", "historicalweather"],
    dag=dag,
    doc_md=task_slack_report_string.format(
        f"Runs glue job on the stage 1 historicalweather, files which reduces them to 17 parquet files and writes them to stage 2",
        "Do not rerun until root cause is understood, data will correct overnight given cause is resolved.",
        "Rerunning could cause other tasks to finish later than required.",
    ),
)

weather_ref_job = PythonOperator(
    task_id="weather_ref_job",
    python_callable=run_glue_job_await_completion,
    op_args=["_process_ref_tables", "historicalweather"],
    dag=dag,
    doc_md=task_slack_report_string.format(
        f"Runs glue job on the stage 1 historicalweather, files which reduces them to 17 parquet files and writes them to stage 2",
        "Do not rerun until root cause is understood, data will correct overnight given cause is resolved.",
        "Rerunning could cause other tasks to finish later than required.",
    ),
)

weather_historical_daily_verify = PythonOperator(
    task_id="weather_historical_daily_verify_task",
    python_callable=fn_weather_historical_daily_verify,
    dag=dag,
)


directory = util.get_dir()

fetch_stage1_data >> weather_staging_job >> weather_ref_job >> weather_historical_daily_verify

if environment in ["preprod", "dev"]:
    master_source = util.get_master_source("weather_historical")

    dest_prefix = directory["s3_weather_key"]["HistoricalWeather"]
    destination = "s3://{}/{}".format(directory["s3_bucket"], dest_prefix)

    source_dir = util.get_dir("newprod")
    source_prefix = source_dir["s3_weather_key"]["HistoricalWeather"]
    source = "s3://{}/{}".format(source_dir["s3_bucket"], source_prefix)

    mirror_s1_data = PythonOperator(
        task_id="mirror_s1_data",
        python_callable=util.process_s3_mirror_job,
        op_kwargs={
            "job_name": "mirror_s1_job",
            "source_input": source,
            "destination_input": destination,
        },
        dag=dag,
    )
    mirror_s1_data >> weather_staging_job
