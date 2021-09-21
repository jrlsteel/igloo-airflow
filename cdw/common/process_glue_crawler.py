import boto3
from time import sleep
import sys
import sentry_sdk

from cdw.connections import connect_db as db


def run_glue_crawler(crawler_name):

    try:
        glue_client = db.get_glue_connection()
        glue_client.start_crawler(Name=crawler_name)
        # may need to wait a little while for the crawler to transition
        # running state or whatever
        current_crawler = glue_client.get_crawler(Name=crawler_name)
        current_crawler_status = current_crawler["Crawler"]["State"]
        while current_crawler_status != "READY":
            sleep(30)
            current_crawler = glue_client.get_crawler(Name=crawler_name)
            current_crawler_status = current_crawler["Crawler"]["State"]
    except Exception as e:
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush(5)
        raise e


if __name__ == "__main__":
    job_gluecrawler_response = run_glue_crawler("data-crawler-weather-forecast-daily-stage2")

    print(job_gluecrawler_response)
