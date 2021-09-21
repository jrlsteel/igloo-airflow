import sys
import subprocess
import timeit
import traceback
from datetime import datetime
from cdw.common import utils as util

iglog = util.IglooLogger()


from cdw.common import utils as util


def process_all_ensek_scripts():

    # Aliases are different on different OS
    pythonAlias = util.get_pythonAlias()

    # print("{0}: >>>> Account Status <<<<".format(datetime.now().strftime('%H:%M:%S')))
    # try:
    #     job_id = util.get_jobID()
    #     util.batch_logging_insert(job_id, 41, 'ensek_account_status_pyscript', 'process_ensek_account_status.py')
    #
    #     start = timeit.default_timer()
    #     subprocess.run([pythonAlias, "processEnsekStatus/process_ensek_account_status.py"] , check=True)
    #     print("{0}: Account Status completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
    #                                                                           float(timeit.default_timer() - start)))
    #     util.batch_logging_update(job_id, 'e')
    # except:
    #     raise

    print("{0}: >>>> Status Registrations Meterpoints <<<<".format(datetime.now().strftime("%H:%M:%S")))
    try:
        job_id = util.get_jobID()
        util.batch_logging_insert(
            job_id,
            3,
            "ensek_registration_meterpoint_status_pyscript",
            "process_ensek_registration_meterpoint_status.py",
        )
        start = timeit.default_timer()
        subprocess.run([pythonAlias, "processEnsekStatus/process_ensek_registration_meterpoint_status.py"], check=True)
        print(
            "{0}: Status Registrations completed in {1:.2f} seconds".format(
                datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start)
            )
        )
        util.batch_logging_update(job_id, "e")
    except:
        print("processAllEnsekScripts - Status Registrations Meterpoints raised an error")
        iglog.in_prod_env(traceback.print_exc())
        raise

    print("{0}: >>>> Internal Estimates <<<<".format(datetime.now().strftime("%H:%M:%S")))
    try:
        job_id = util.get_jobID()
        util.batch_logging_insert(job_id, 4, "ensek_internal_estimates_pyscript", "process_ensek_internal_estimates.py")
        start = timeit.default_timer()
        subprocess.run([pythonAlias, "processEnsekEstimates/process_ensek_internal_estimates.py"], check=True)
        print(
            "{0}: Internal Estimates completed in {1:.2f} seconds".format(
                datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start)
            )
        )
        util.batch_logging_update(job_id, "e")
    except:
        print("processAllEnsekScripts - Internal Estimates raised an error")
        iglog.in_prod_env(traceback.print_exc())
        raise

    print("{0}: >>>> Tariff History <<<<".format(datetime.now().strftime("%H:%M:%S")))
    try:
        job_id = util.get_jobID()
        util.batch_logging_insert(job_id, 5, "ensek_tariff_history_pyscript", "process_ensek_tariffs_history.py")
        start = timeit.default_timer()
        subprocess.run([pythonAlias, "processEnsekTariffs/process_ensek_tariffs_history.py"], check=True)
        print(
            "{0}: Tariff History completed in {1:.2f} seconds".format(
                datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start)
            )
        )
        util.batch_logging_update(job_id, "e")
    except:
        print("processAllEnsekScripts - Tariff History raised an error")
        iglog.in_prod_env(traceback.print_exc())
        raise

    print("{0}: >>>> Account Settings <<<<".format(datetime.now().strftime("%H:%M:%S")))
    try:
        job_id = util.get_jobID()
        util.batch_logging_insert(job_id, 12, "ensek_account_settings_pyscript", "process_ensek_account_settings.py")
        start = timeit.default_timer()
        subprocess.run([pythonAlias, "processEnsekAccountSettings/process_ensek_account_settings.py"], check=True)
        print(
            "{0}: Account Settings completed in {1:.2f} seconds".format(
                datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start)
            )
        )
        util.batch_logging_update(job_id, "e")
    except:
        print("processAllEnsekScripts - Account Settings raised an error")
        iglog.in_prod_env(traceback.print_exc())
        raise

    print("{0}: >>>> Account Transactions <<<<".format(datetime.now().strftime("%H:%M:%S")))
    try:
        job_id = util.get_jobID()
        util.batch_logging_insert(job_id, 6, "ensek_transactions_pyscript", "process_ensek_transactions.py")
        start = timeit.default_timer()
        subprocess.run([pythonAlias, "processEnsekTransactions/process_ensek_transactions.py"], check=True)
        print(
            "{0}: Account Transactions completed in {1:.2f} seconds".format(
                datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start)
            )
        )
        util.batch_logging_update(job_id, "e")

    except:
        print("processAllEnsekScripts - Account Transactions raised an error")
        iglog.in_prod_env(traceback.print_exc())
        raise

    return True


if __name__ == "__main__":
    response = process_all_ensek_scripts()
