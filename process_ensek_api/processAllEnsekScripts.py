import sys
import os
import platform
import subprocess
import timeit
from datetime import datetime


sys.path.append('..')
from common import utils as util


def process_all_ensek_scripts():

    # Aliases are different on different OS
    pythonAlias = util.get_pythonAlias()

    print("{0}: >>>> Meter Points <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        start = timeit.default_timer()
        subprocess.run([pythonAlias, "processEnsekApiMeterPointsData.py"])
        print("{0}: Meter Points completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
    except:
        raise

    print("{0}: >>>> Status Registrations <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        start = timeit.default_timer()
        subprocess.run([pythonAlias, "processEnsekApiStatusRegistrationsData.py"])
        print("{0}: Status Registrations completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
    except:
        raise

    print("{0}: >>>> Internal Readings <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        start = timeit.default_timer()
        subprocess.run([pythonAlias, "processEnsekApiInternalReadingsData.py"])
        print("{0}: Internal Readings completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
    except:
        raise

    print("{0}: >>>> Internal Estimates <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        start = timeit.default_timer()
        subprocess.run([pythonAlias, "processEnsekApiInternalEstimatesData.py"])
        print("{0}: Internal Estimates completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
    except:
        raise

    print("{0}: >>>> Tariff History <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        start = timeit.default_timer()
        subprocess.run([pythonAlias, "processEnsekApiTariffsWithHistory.py"])
        print("{0}: Tariff History completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
    except:
        raise

    print("{0}: >>>> Direct Debits <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        start = timeit.default_timer()
        subprocess.run([pythonAlias, "processEnsekApiDirectDebits.py"])
        print("{0}: Direct Debits completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
    except:
        raise

    return True


if __name__ == '__main__':
    response = process_all_ensek_scripts()