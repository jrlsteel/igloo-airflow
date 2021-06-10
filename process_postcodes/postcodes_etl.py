import timeit
import requests
import json
from pandas.io.json import json_normalize
import time
from requests import ConnectionError
import csv
import sys
import os
import datetime

from requests.exceptions import Timeout

from requests.packages.urllib3.util.retry import Retry
import tempfile
from ratelimit import limits, sleep_and_retry
import pandas as pd

sys.path.append("..")

from common.etl import ETLPipeline, TimeoutHTTPAdapter
from common import utils
from conf import config as con
from connections.connect_db import get_boto_S3_Connections as s3_con
from common.utils import IglooLogger


class PostcodesETLPipeline(ETLPipeline):
    def __init__(self, source, destination):
        super().__init__("postcodes")

        self.source = source
        self.destination = destination

    def extract(self):
        """
        The Extract stage of the ETL pipeline.

        Downloads the data from the remote API, and stores it in a local temp file.
        """
        session = requests.Session()

        # Configure a retry strategy so we get automatic retries when we receive
        # appropriate status codes.
        retry_strategy = Retry(
            total=3,
            # Retry if any of these status codes are received
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "OPTIONS"],
            # Use an exponential backoff
            backoff_factor=1,
        )
        adapter = TimeoutHTTPAdapter(max_retries=retry_strategy)

        session.mount("https://", adapter)
        session.mount("http://", adapter)

        # Download the file from the remote API and stream it in to a temporary file.
        # When it is downloaded, read it in to a DataFrame and return it.
        with tempfile.NamedTemporaryFile() as fp:
            self.logger.in_prod_env("GET {}".format(self.source["url"]))
            response = session.get(self.source["url"], stream=True)
            self.logger.in_prod_env("Received response with status_code: {}".format(response.status_code))

            if response.status_code >= 200 and response.status_code <= 299:
                total_bytes = 0
                total_chunks = 0
                for chunk in response.iter_content(chunk_size=None):
                    if chunk:
                        bytes = fp.write(chunk)
                        total_bytes += bytes
                        total_chunks += 1
                self.logger.in_prod_env("Total chunks: {}".format(total_chunks))
                self.logger.in_prod_env("Total bytes: {}".format(total_bytes))

                # We need to rewind to the start of the file so that we can read it in.
                fp.seek(0)

                dtypes = self.destination["extract_dtypes"]
                columns = dtypes.keys()
                return pd.read_csv(fp).astype(dtypes)[columns]
            else:
                raise Exception("GET {} failed with status code {}".format(self.source["url"], response.status_code))

    def transform(self, df):
        """
        The Transform stage of the ETL pipeline.

        Ensures the data has the correct types and column ordering, and appends
        an etl_change column.
        """
        df["etl_change"] = datetime.datetime.now()
        dtypes = self.destination["transform_dtypes"]
        columns = dtypes.keys()
        return df.astype(dtypes)[columns]

    def load(self, df):
        """
        The Load stage of the ETL pipeline.

        Writes the data out to S3.
        """
        self.logger.in_prod_env(
            "Storing data in S3: s3://{}/{}".format(self.destination["s3_bucket"], self.destination["s3_key"])
        )

        def progress_cb(bytes_sent, total_bytes):
            self.logger.in_test_env("Uploaded {} of {} bytes".format(bytes_sent, total_bytes))

        s3_object = s3_con(self.destination["s3_bucket"])
        s3_object.key = self.destination["s3_key"]

        s3_object.set_contents_from_string(df.to_csv(None, index=False), cb=progress_cb)

    def run(self):
        """
        Runs the three stages of the ETL pipeline.
        """
        start_time = timeit.default_timer()
        df = self.extract()
        self.logger.in_prod_env("Completed extract step in {} seconds".format(timeit.default_timer() - start_time))

        start_time = timeit.default_timer()
        df = self.transform(df)
        self.logger.in_prod_env("Completed transform step in {} seconds".format(timeit.default_timer() - start_time))

        start_time = timeit.default_timer()
        self.load(df)
        self.logger.in_prod_env("Completed load step in {} seconds".format(timeit.default_timer() - start_time))

    def log_error(self, error_msg, error_code=""):
        self.logger.in_prod_env(error_msg)
        logfile = os.path.join(sys.path[0], "logs", "{}_logs_{}.csv".format(self.name, time.strftime("%d%m%Y")))
        os.makedirs(os.path.dirname(logfile), exist_ok=True)

        with open(logfile, mode="a") as errorlog:
            csv_writer = csv.writer(errorlog, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csv_writer.writerow([error_msg, error_code])


if __name__ == "__main__":

    start = timeit.default_timer()

    logger = IglooLogger(source="postcodes")

    logger.in_prod_env("Starting postcodes ETL")

    p = PostcodesETLPipeline(
        source={"url": utils.get_common_info("postcodes", "api_url")},
        destination={
            "s3_bucket": utils.get_dir()["s3_bucket"],
            "s3_key": utils.get_common_info("postcodes", "s3_key"),
            "extract_dtypes": utils.get_common_info("postcodes", "extract_dtypes"),
            "transform_dtypes": utils.get_common_info("postcodes", "transform_dtypes"),
        },
    )

    try:
        p.run()
        logger.in_prod_env("Process completed in {} seconds".format(timeit.default_timer() - start))
    except Exception as e:
        logger.in_prod_env("Process failed after {} seconds".format(timeit.default_timer() - start))
        raise Exception from e
