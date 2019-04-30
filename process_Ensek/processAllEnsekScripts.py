import sys
import subprocess
import timeit
from datetime import datetime


sys.path.append('..')
from common import utils as util



def process_all_ensek_scripts():

    # Aliases are different on different OS
    pythonAlias = util.get_pythonAlias()

    print("{0}: >>>> Account Status <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        job_id = util.get_jobID()
        util.batch_logging_insert(job_id, 41, 'ensek_account_status_pyscript', 'process_ensek_account_status.py')

        start = timeit.default_timer()
        subprocess.run([pythonAlias, "processEnsekStatus/process_ensek_account_status.py"])
        print("{0}: Account Status completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                              float(timeit.default_timer() - start)))
        util.batch_logging_update(job_id, 'e')
    except:
        raise

    print("{0}: >>>> Status Registrations Meterpoints <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        job_id = util.get_jobID()
        util.batch_logging_insert(job_id, 3, 'ensek_registration_meterpoint_status_pyscript', 'process_ensek_registration_meterpoint_status.py')
        start = timeit.default_timer()
        subprocess.run([pythonAlias, "processEnsekStatus/process_ensek_registration_meterpoint_status.py"])
        print("{0}: Status Registrations completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
        util.batch_logging_update(job_id, 'e')
    except:
        raise

    print("{0}: >>>> Internal Estimates <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        job_id = util.get_jobID()
        util.batch_logging_insert(job_id, 4, 'ensek_internal_estimates_pyscript','process_ensek_internal_estimates.py')
        start = timeit.default_timer()
        subprocess.run([pythonAlias, "processEnsekEstimates/process_ensek_internal_estimates.py"])
        print("{0}: Internal Estimates completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
        util.batch_logging_update(job_id, 'e')
    except:
        raise

    print("{0}: >>>> Tariff History <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        job_id = util.get_jobID()
        util.batch_logging_insert(job_id, 5, 'ensek_tariff_history_pyscript', 'process_ensek_tariffs_history.py')
        start = timeit.default_timer()
        subprocess.run([pythonAlias, "processEnsekTariffs/process_ensek_tariffs_history.py"])
        print("{0}: Tariff History completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
        util.batch_logging_update(job_id, 'e')
    except:
        raise

    print("{0}: >>>> Account Transactions <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        job_id = util.get_jobID()
        util.batch_logging_insert(job_id, 6, 'ensek_transactions_pyscript', 'process_ensek_transactions.py')
        start = timeit.default_timer()
        subprocess.run([pythonAlias, "processEnsekTransactions/process_ensek_transactions.py"])
        print("{0}: Account Transactions completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                       float(timeit.default_timer() - start)))
        util.batch_logging_update(job_id, 'e')

    except:
        raise

    return True


if __name__ == '__main__':
    response = process_all_ensek_scripts()