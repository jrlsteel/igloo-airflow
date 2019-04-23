import sys
import subprocess
import timeit
from datetime import datetime


sys.path.append('..')
from common import utils as util



def process_all_ensek_scripts():

    # Aliases are different on different OS
    pythonAlias = util.get_pythonAlias()


    #not related to PA
    # print("{0}: >>>> Meter Points History<<<<".format(datetime.now().strftime('%H:%M:%S')))
    # try:
    #     start = timeit.default_timer()
    #     subprocess.run([pythonAlias, "processEnsekMeterpoints/process_ensek_meterpoints.py"])
    #     print("{0}: Meter Points completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
    #                                                                   float(timeit.default_timer() - start)))
    # except:
    #     raise

    print("{0}: >>>> Status Registrations Meterpoints <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        start = timeit.default_timer()
        subprocess.run([pythonAlias, "processEnsekRegistrationStatus/process_ensek_registration_meterpoint_status.py"])
        print("{0}: Status Registrations completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
    except:
        raise

    print("{0}: >>>> Internal Estimates <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        start = timeit.default_timer()
        subprocess.run([pythonAlias, "processEnsekEstimates/process_ensek_internal_estimates.py"])
        print("{0}: Internal Estimates completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
    except:
        raise

    print("{0}: >>>> Tariff History <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        start = timeit.default_timer()
        subprocess.run([pythonAlias, "processEnsekTariffs/process_ensek_tariffs_history.py"])
        print("{0}: Tariff History completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
    except:
        raise

    # print("{0}: >>>> Direct Debits <<<<".format(datetime.now().strftime('%H:%M:%S')))
    # try:
    #     start = timeit.default_timer()
    #     subprocess.run([pythonAlias, "processEnsekApiDirectDebits.py"])
    #     print("{0}: Direct Debits completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
    # except:
    #     raise

    # print("{0}: >>>> Live Balances <<<<".format(datetime.now().strftime('%H:%M:%S')))
    # try:
    #     start = timeit.default_timer()
    #     subprocess.run([pythonAlias, "process_ensek_accounts_live_balances.py"])
    #     print("{0}: Live Balances completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
    #                                                                    float(timeit.default_timer() - start)))
    # except:
    #     raise

    print("{0}: >>>> Account Transactions <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        start = timeit.default_timer()
        subprocess.run([pythonAlias, "processEnsekTransactions/process_ensek_transactions.py"])
        print("{0}: Account Transactions completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                       float(timeit.default_timer() - start)))
    except:
        raise

    return True


if __name__ == '__main__':
    response = process_all_ensek_scripts()