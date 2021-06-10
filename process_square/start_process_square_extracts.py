import sys
from datetime import datetime
import timeit
import subprocess
import json
import gocardless_pro
from square.client import Client
from queue import Queue
from pandas.io.json import json_normalize
import random
from retrying import retry

sys.path.append("../..")
from common import utils as util
from conf import config as con
from connections.connect_db import get_finance_s3_connections as s3_con
from connections import connect_db as db

client = Client(
    access_token=con.square["access_token"],
    environment=con.square["environment"],
)
payments_api = client.payments


class StartSquareAPIExtracts:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.all_jobid = util.get_jobID()
        self.goCardless_jobid = util.get_jobID()
        self.square_jobid = util.get_jobID()

    def retry_function(self, process):
        for i in range(0, 3):
            while True:
                try:
                    process
                except (
                    json.decoder.JSONDecodeError,
                    gocardless_pro.errors.GoCardlessInternalError,
                    gocardless_pro.errors.MalformedResponseError,
                ) as e:
                    continue
                break

    @retry(wait_random_min=1000, wait_random_max=2000)
    def retry_function2(self, process):
        return process

    def extract_square_payments_job(self):
        """
        Calls the GoCardless Subscriptions API extract: go_cardless_customers.py.
        :return: None
        """

        print("{0}: >>>> Process Square Payments API extract  <<<<".format(datetime.now().strftime("%H:%M:%S")))
        try:
            ##util.batch_logging_insert(self.square_jobid, 401, 'process_square_payments.py', 'start_process_square_extracts.py')
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "process_square_payments.py"], check=True)
            # util.batch_logging_update(self.square_jobid, 'e')
            print(
                "{0}: Process Square Payments API extract completed in {1:.2f} seconds".format(
                    datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start)
                )
            )
        except Exception as e:
            # util.batch_logging_update(self.square_jobid, 'f', str(e))
            # util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Process Square Payments API extract :- " + str(e))
            sys.exit(1)


if __name__ == "__main__":

    s = StartSquareAPIExtracts()

    # util.batch_logging_insert(s.all_jobid, 403, 'all_square_api_jobs', 'start_square_api_extracts.py')

    ## Square Payments API Endpoint
    print("{0}:  Square Payments API extract running...".format(datetime.now().strftime("%H:%M:%S")))
    ##s.retry_function(process = s.extract_square_payments_job())
    s.retry_function(process=s.extract_square_payments_job())

    print("{0}: All Square API extracts completed successfully".format(datetime.now().strftime("%H:%M:%S")))
    # util.batch_logging_update(s.all_jobid, 'e')
