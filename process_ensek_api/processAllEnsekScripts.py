import sys
import os
import subprocess
import timeit
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def process_all_ensek_scripts():

    print("{0}: >>>> Meter Points <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        start = timeit.default_timer()
        subprocess.call(["python3", "processEnsekApiMeterPointsData.py"])
        print("{0}: Meter Points completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
    except:
        raise

    print("{0}: >>>> Status Registrations <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        start = timeit.default_timer()
        subprocess.call(["python3", "processEnsekApiStatusRegistrationsData.py"])
        print("{0}: Status Registrations completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
    except:
        raise

    print("{0}: >>>> Internal Readings <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        start = timeit.default_timer()
        subprocess.call(["python3", "processEnsekApiInternalReadingsData.py"])
        print("{0}: Internal Readings completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
    except:
        raise

    print("{0}: >>>> Internal Estimates <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        start = timeit.default_timer()
        subprocess.call(["python3", "processEnsekApiInternalEstimatesData.py"])
        print("{0}: Internal Estimates completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
    except:
        raise

    print("{0}: >>>> Tariff History <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        start = timeit.default_timer()
        subprocess.call(["python3", "processEnsekApiTariffsWithHistory.py"])
        print("{0}: Tariff History completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
    except:
        raise

    print("{0}: >>>> Direct Debits <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        start = timeit.default_timer()
        subprocess.call(["python3", "processEnsekApiDirectDebits.py"])
        print("{0}: Direct Debits completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
    except:
        raise

    return True


if __name__ == '__main__':
    response = process_all_ensek_scripts()