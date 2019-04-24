import sys
import subprocess
import timeit
from datetime import datetime

sys.path.append('..')
from common import utils as util


def process_all_ensek_pa_scripts():

    # Aliases are different on different OS
    pythonAlias = util.get_pythonAlias()


    print("{0}: >>>> Meter Points <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        # Batch Logging
        job_id = util.get_jobID()
        util.batch_logging_insert(job_id, 1, 'ensek_meterpoints_pyscript', 'process_ensek_meterpoints_no_history.py')
        start = timeit.default_timer()
        subprocess.run([pythonAlias, "processEnsekMeterpoints/process_ensek_meterpoints_no_history.py"])
        print("{0}: Meter Points completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
        util.batch_logging_update(job_id, 'e')

    except:
        raise

    print("{0}: >>>> Internal Readings <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        job_id = util.get_jobID()
        # Batch Logging
        util.batch_logging_insert(job_id, 1, 'ensek_internal_readings_pyscript', 'process_ensek_internal_readings.py')
        start = timeit.default_timer()
        subprocess.run([pythonAlias, "processEnsekReadings/process_ensek_internal_readings.py"])
        print("{0}: Internal Readings completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                           float(timeit.default_timer() - start)))
        util.batch_logging_update(job_id, 'e')
    except:
        raise

    return True


if __name__ == '__main__':
    response = process_all_ensek_pa_scripts()
