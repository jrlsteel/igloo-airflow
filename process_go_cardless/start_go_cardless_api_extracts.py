import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append('../..')
from common import utils as util


class StartGoCardlessAPIExtracts:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()

    def extract_go_cardless_payments_job(self):
        """
        Calls the GoCardless Payments API extract: go_cardless_payments.py.
        :return: None
        """

        print("{0}: >>>> Process Go-Cardless Payments API extract  <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "go_cardless_payments.py"], check=True)
            print("{0}: Process Go-Cardless Payments API extract completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start)))
        except Exception as e:
            print("Error in Process Go-Cardless Payments API extract :- " + str(e))
            sys.exit(1)

    def extract_go_cardless_refunds_job(self):
        """
        Calls the GoCardless Refunds API extract: go_cardless_refunds.py.
        :return: None
        """

        print("{0}: >>>> Process Go-Cardless Refunds API extract  <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "go_cardless_refunds.py"], check=True)
            print("{0}: Process Go-Cardless Refunds API extract completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start)))
        except Exception as e:
            print("Error in Process Go-Cardless Refunds API extract :- " + str(e))
            sys.exit(1)

    def extract_go_cardless_mandates_job(self):
        """
        Calls the GoCardless Mandates API extract: go_cardless_mandates.py.
        :return: None
        """

        print("{0}: >>>> Process Go-Cardless Mandates API extract  <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "go_cardless_mandates.py"], check=True)
            print("{0}: Process Go-Cardless Mandates API extract completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start)))
        except Exception as e:
            print("Error in Process Go-Cardless Mandates API extract :- " + str(e))
            sys.exit(1)

    def extract_go_cardless_payout_job(self):
        """
        Calls the GoCardless Payouts API extract: go_cardless_payouts.py.
        :return: None
        """

        print("{0}: >>>> Process Go-Cardless Payouts API extract  <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "go_cardless_payout.py"], check=True)
            print("{0}: Process Go-Cardless Payout API extract completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start)))
        except Exception as e:
            print("Error in Process Go-Cardless Payout API extract :- " + str(e))
            sys.exit(1)

    def extract_go_cardless_events_job(self):
        """
        Calls the GoCardless Events API extract: go_cardless_events.py.
        :return: None
        """

        print("{0}: >>>> Process Go-Cardless Events API extract  <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "go_cardless_events.py"], check=True)
            print("{0}: Process Go-Cardless Events API extract completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start)))
        except Exception as e:
            print("Error in Process Go-Cardless events API extract :- " + str(e))
            sys.exit(1)

    def extract_go_cardless_customers_job(self):
        """
        Calls the GoCardless Clients API extract: go_cardless_customers.py.
        :return: None
        """

        print("{0}: >>>> Process Go-Cardless Clients API extract  <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "go_cardless_customers.py"], check=True)
            print("{0}: Process Go-Cardless Clients API extract completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start)))
        except Exception as e:
            print("Error in Process Go-Cardless Clients API extract :- " + str(e))
            sys.exit(1)

    def extract_go_cardless_subscriptions_job(self):
        """
        Calls the GoCardless Subscriptions API extract: go_cardless_customers.py.
        :return: None
        """

        print("{0}: >>>> Process Go-Cardless Subscriptions API extract  <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "go_cardless_subscriptions.py"], check=True)
            print("{0}: Process Go-Cardless Clients API extract completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start)))
        except Exception as e:
            print("Error in Process Go-Cardless Subscriptions API extract :- " + str(e))
            sys.exit(1)



if __name__ == '__main__':

    s = StartGoCardlessAPIExtracts()

    ## Payments API Endpoint
    print("{0}:  Go-Cardless Payments API extract running...".format(datetime.now().strftime('%H:%M:%S')))
    s.extract_go_cardless_payments_job()

    ## Refunds API Endpoint
    print("{0}:  Go-Cardless Refunds API extract running...".format(datetime.now().strftime('%H:%M:%S')))
    s.extract_go_cardless_refunds_job()

    ## Payouts API Endpoint
    print("{0}:  Go-Cardless Payouts API extract running...".format(datetime.now().strftime('%H:%M:%S')))
    s.extract_go_cardless_payouts_job()

    ## Mandates API Endpoint
    print("{0}:  Go-Cardless Mandates API extract running...".format(datetime.now().strftime('%H:%M:%S')))
    s.extract_go_cardless_mandates_job()

    ## Events API Endpoint
    print("{0}:  Go-Cardless Event API extract running...".format(datetime.now().strftime('%H:%M:%S')))
    s.extract_go_cardless_events_job()

    ## Clients API Endpoint
    print("{0}:  Go-Cardless Customers API extract running...".format(datetime.now().strftime('%H:%M:%S')))
    s.extract_go_cardless_customers_job()

    ## Subscriptions API Endpoint
    print("{0}:  Go-Cardless Event API extract running...".format(datetime.now().strftime('%H:%M:%S')))
    s.extract_go_cardless_subscriptions_job()



    print("{0}: All Go-Cardless API extracts completed successfully".format(datetime.now().strftime('%H:%M:%S')))

