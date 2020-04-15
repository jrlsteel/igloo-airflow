import sys
from datetime import datetime
import timeit
import subprocess
import json
import gocardless_pro
from square.client import Client
from queue import Queue
from pandas.io.json import json_normalize

sys.path.append('../..')
from common import process_glue_job as glue
from common import utils as util
import process_square
from process_square import process_square_payments


class StartGoCardlessAPIExtracts:
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
                except (json.decoder.JSONDecodeError, gocardless_pro.errors.GoCardlessInternalError,
                gocardless_pro.errors.MalformedResponseError) as e:
                    continue
                break


    def extract_go_cardless_payments_job(self):
        """
        Calls the GoCardless Payments API extract: go_cardless_payments.py.
        :return: None
        """

        print("{0}: >>>> Process Go-Cardless Payments API extract  <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            util.batch_logging_insert(self.goCardless_jobid, 400, 'go_cardless_payments.py', 'start_go_cardless_api_extracts.py')
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "go_cardless_payments.py"], check=True)
            util.batch_logging_update(self.goCardless_jobid, 'e')
            print("{0}: Process Go-Cardless Payments API extract completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start)))
        except Exception as e:
            util.batch_logging_update(self.goCardless_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Process Go-Cardless Payments API extract :- " + str(e))
            sys.exit(1)

    def extract_go_cardless_refunds_job(self):
        """
        Calls the GoCardless Refunds API extract: go_cardless_refunds.py.
        :return: None
        """

        print("{0}: >>>> Process Go-Cardless Refunds API extract  <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            util.batch_logging_insert(self.goCardless_jobid, 400, 'go_cardless_refunds.py',
                                      'start_go_cardless_api_extracts.py')
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "go_cardless_refunds.py"], check=True)
            util.batch_logging_update(self.goCardless_jobid, 'e')
            print("{0}: Process Go-Cardless Refunds API extract completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start)))
        except Exception as e:
            util.batch_logging_update(self.goCardless_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Process Go-Cardless Refunds API extract :- " + str(e))
            sys.exit(1)

    def extract_go_cardless_mandates_job(self):
        """
        Calls the GoCardless Mandates API extract: go_cardless_mandates.py.
        :return: None
        """

        print("{0}: >>>> Process Go-Cardless Mandates API extract  <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            util.batch_logging_insert(self.goCardless_jobid, 400, 'go_cardless_mandates.py',
                                      'start_go_cardless_api_extracts.py')
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "go_cardless_mandates.py"], check=True)
            util.batch_logging_update(self.goCardless_jobid, 'e')
            print("{0}: Process Go-Cardless Mandates API extract completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start)))
        except Exception as e:
            util.batch_logging_update(self.goCardless_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Process Go-Cardless Mandates API extract :- " + str(e))
            sys.exit(1)

    def extract_go_cardless_payouts_job(self):
        """
        Calls the GoCardless Payouts API extract: go_cardless_payouts.py.
        :return: None
        """

        print("{0}: >>>> Process Go-Cardless Payouts API extract  <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            util.batch_logging_insert(self.goCardless_jobid, 400, 'go_cardless_payout.py',
                                      'start_go_cardless_api_extracts.py')
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "go_cardless_payout.py"], check=True)
            util.batch_logging_update(self.goCardless_jobid, 'e')
            print("{0}: Process Go-Cardless Payouts API extract completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start)))
        except Exception as e:
            util.batch_logging_update(self.goCardless_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Process Go-Cardless Payouts API extract :- " + str(e))
            sys.exit(1)

    def extract_go_cardless_events_job(self):
        """
        Calls the GoCardless Events API extract: go_cardless_events.py.
        :return: None
        """

        print("{0}: >>>> Process Go-Cardless Events API extract  <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            util.batch_logging_insert(self.goCardless_jobid, 400, 'go_cardless_events.py',
                                      'start_go_cardless_api_extracts.py')
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "go_cardless_events.py"], check=True)
            util.batch_logging_update(self.goCardless_jobid, 'e')
            print("{0}: Process Go-Cardless Events API extract completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start)))
        except Exception as e:
            util.batch_logging_update(self.goCardless_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Process Go-Cardless events API extract :- " + str(e))
            sys.exit(1)

    def extract_go_cardless_customers_job(self):
        """
        Calls the GoCardless Clients API extract: go_cardless_customers.py.
        :return: None
        """

        print("{0}: >>>> Process Go-Cardless Clients API extract  <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            util.batch_logging_insert(self.goCardless_jobid, 400, 'go_cardless_customers.py',
                                      'start_go_cardless_api_extracts.py')
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "go_cardless_customers.py"], check=True)
            util.batch_logging_update(self.goCardless_jobid, 'e')
            print("{0}: Process Go-Cardless Clients API extract completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start)))
        except Exception as e:
            util.batch_logging_update(self.goCardless_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Process Go-Cardless Clients API extract :- " + str(e))
            sys.exit(1)

    def extract_go_cardless_subscriptions_job(self):
        """
        Calls the GoCardless Subscriptions API extract: go_cardless_customers.py.
        :return: None
        """

        print("{0}: >>>> Process Go-Cardless Subscriptions API extract  <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            util.batch_logging_insert(self.goCardless_jobid, 400, 'go_cardless_subscriptions.py',
                                      'start_go_cardless_api_extracts.py')
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "go_cardless_subscriptions.py"], check=True)
            util.batch_logging_update(self.goCardless_jobid, 'e')
            print("{0}: Process Go-Cardless Clients API extract completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start)))
        except Exception as e:
            util.batch_logging_update(self.goCardless_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Process Go-Cardless Subscriptions API extract :- " + str(e))
            sys.exit(1)

    def extract_square_payments_job(self):
        """
        Calls the GoCardless Subscriptions API extract: go_cardless_customers.py.
        :return: None
        """

        print("{0}: >>>> Process Square Payments API extract  <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            util.batch_logging_insert(self.square_jobid, 401, 'process_square_payments.py',
                                      'start_go_cardless_api_extracts.py')
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "../process_square/process_square_payments.py"], check=True)
            util.batch_logging_update(self.square_jobid, 'e')
            print("{0}: Process Square Payments API extract completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start)))
        except Exception as e:
            util.batch_logging_update(self.square_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Process Square Payments API extract :- " + str(e))
            sys.exit(1)



if __name__ == '__main__':

    s = StartGoCardlessAPIExtracts()

    util.batch_logging_insert(s.all_jobid, 402, 'all_go_cardless_api_jobs', 'start_go_cardless_api_extracts.py')

    ## Payments API Endpoint
    print("{0}:  Go-Cardless Payments API extract running...".format(datetime.now().strftime('%H:%M:%S')))
    s.retry_function(process = s.extract_go_cardless_payments_job())


    ## Refunds API Endpoint
    print("{0}:  Go-Cardless Refunds API extract running...".format(datetime.now().strftime('%H:%M:%S')))
    s.retry_function(process = s.extract_go_cardless_refunds_job())


    ## Payouts API Endpoint
    print("{0}:  Go-Cardless Payouts API extract running...".format(datetime.now().strftime('%H:%M:%S')))
    s.retry_function(process = s.extract_go_cardless_payouts_job())


    ## Mandates API Endpoint
    print("{0}:  Go-Cardless Mandates API extract running...".format(datetime.now().strftime('%H:%M:%S')))
    s.retry_function(process = s.extract_go_cardless_mandates_job())


    ## Events API Endpoint
    print("{0}:  Go-Cardless Event API extract running...".format(datetime.now().strftime('%H:%M:%S')))
    s.retry_function(process= s.extract_go_cardless_events_job())


    ## Clients API Endpoint
    print("{0}:  Go-Cardless Customers API extract running...".format(datetime.now().strftime('%H:%M:%S')))
    s.retry_function(process= s.extract_go_cardless_customers_job())


    ## Subscriptions API Endpoint
    print("{0}:  Go-Cardless Subscriptions API extract running...".format(datetime.now().strftime('%H:%M:%S')))
    s.retry_function(process= s.extract_go_cardless_subscriptions_job())




    print("{0}: All Go-Cardless API extracts completed successfully".format(datetime.now().strftime('%H:%M:%S')))
    util.batch_logging_update(s.all_jobid, 'e')

