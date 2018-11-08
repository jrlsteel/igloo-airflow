import sys
import os
import subprocess

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def process_all_ensek_scripts():

    print(">>>> Meter Points <<<<")
    try:
        subprocess.call(["python3", "processEnsekApiMeterPointsData.py"])
    except:
        raise

    print(">>>> Status Registrations <<<<")
    try:
        subprocess.call(["python3", "processEnsekApiStatusRegistrationsData.py"])
    except:
        raise

    print(">>>> Internal Readings <<<<")
    try:
        subprocess.call(["python3", "processEnsekApiInternalReadingsData.py"])
    except:
        raise

    print(">>>> Internal Estimates <<<<")
    try:
        subprocess.call(["python3", "processEnsekApiInternalEstimatesData.py"])
    except:
        raise

    print(">>>> Tariff Histtory <<<<")
    try:
        subprocess.call(["python3", "processEnsekApiTariffsWithHistory.py"])
    except:
        raise

    print(">>>> Direct Debits <<<<")
    try:
        subprocess.call(["python3", "processEnsekApiDirectDebits.py"])
    except:
        raise

    return True


if __name__ == '__main__':
    response = process_all_ensek_scripts()