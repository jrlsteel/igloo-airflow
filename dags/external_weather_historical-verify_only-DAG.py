from __future__ import print_function
from datetime import datetime, timedelta

import time
import sys
from builtins import range
from pprint import pprint

from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import PythonVirtualenvOperator
from airflow.operators.bash_operator import BashOperator
import sentry_sdk

sys.path.append("/opt/airflow/enzek-meterpoint-readings")

import common.process_glue_crawler
import common.utils
from conf import config
from common.slack_utils import alert_slack

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2), # don't know what this is doing
    'on_failure_callback': alert_slack
}

dag = DAG(
    dag_id='igloo_weather_historical_verify_only',
    default_args=args,
    schedule_interval=None,
    tags=['cdw'],
    catchup=False,
    max_active_runs=1,
)


def verify_postcode_data_exists_for_weather_station_postcodes():
    #Checks there is postcode data for weather station postcodes in redshift table ref_historical_weather
    try:
        gsp_weather_station_postcodes = [
            'IP7 7RE',
            'NG16 1JF',
            'TW6 2EQ',
            'LL18 5YD',
            'B46 3JH',
            'NE15 0RH',
            'RH6 0EN',
            'CF62 4JD',
            'BA22 8HT',
            'HU17 7LZ',
            'PA7 5NX',
            'AB21 7DU',
            'CM6 3TH',
            'PE8 6HB',
            'CH4 0GZ',
            'CV23 9EU',
            'NE66 3JF',
            'WA14 3SB',
            'HA4 6NG',
            'CR8 5EG',
            'SP4 0JF',
            'DL7 9NJ',
            'KA9 2PL',
            'IV36 3UH'
        ]
        hourly_historical_hours = 24
        expected_row_count = len(gsp_weather_station_postcodes) * hourly_historical_hours
        historical_date = datetime.now()  - timedelta(1)
        historical_date = datetime.strftime(historical_date, '%Y-%m-%d')    

        df = common.utils.execute_query_return_df("""select count(*) from ref_historical_weather where postcode in ('{}') and trunc(timestamp_utc) = '{}'""".format(
            "', '".join(gsp_weather_station_postcodes),
            historical_date
        ))
        assert df['count'][0] >= expected_row_count
    except Exception as e:
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush(5)
        raise e

weather_historical_daily_verify = PythonOperator(
    task_id="weather_historical_daily_verify_task",
    python_callable=verify_postcode_data_exists_for_weather_station_postcodes,
    dag=dag,
)

weather_historical_daily_verify

