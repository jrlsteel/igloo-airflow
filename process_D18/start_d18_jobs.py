import sys
from datetime import datetime
import timeit
import subprocess
import platform

sys.path.append('..')
from common import process_glue_job as d18


def submit_download_d18_job():

    if platform.system() == 'Windows':
        pythonalias = 'python'
    else:
        pythonalias = 'python3'

    print("{0}: >>>> Downloading D18 files <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        start = timeit.default_timer()
        subprocess.run([pythonalias, "download_d18.py"])
        print("{0}: download_d18 completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                      float(timeit.default_timer() - start)))
    except Exception as e:
        print("Error in download_d18 process :- " + str(e))
        sys.exit(1)


def submit_process_d18_job():
    if platform.system() == 'Windows':
        pythonalias = 'python'
    else:
        pythonalias = 'python3'

    print("{0}: >>>> Process D18 files <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        start = timeit.default_timer()
        subprocess.run([pythonalias, "process_d18.py"])
        print("{0}: Process D18 files completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                           float(timeit.default_timer() - start)))
    except Exception as e:
        print("Error in download_d18 process :- " + str(e))
        sys.exit(1)


def submit_staging_gluejob():
    try:
        obj_stage = d18.ProcessGlueJob(job_name='process_staging_files', input_files='d18_files')
        staging_job_response = obj_stage.run_glue_job()
        if staging_job_response:
            print("{0}: Staging Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
            # return staging_job_response
        else:
            print("Error occurred in Staging Job")
            # return staging_job_response
            raise Exception
    except Exception as e:
        print("Error in Staging Job :- " + str(e))
        sys.exit(1)


def submit_d18_gluejob():
    try:
        # staging_job_response = ss.process_staging_job()
        obj_d18 = d18.ProcessGlueJob(job_name='process_ref_d18')
        staging_job_response = obj_d18.run_glue_job()
        if staging_job_response:
            print("{0}: Staging Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
            # return staging_job_response
        else:
            print("Error occurred in Staging Job")
            # return staging_job_response
            raise Exception
    except Exception as e:
        print("Error in Staging Job :- " + str(e))
        sys.exit(1)


if __name__ == '__main__':

    # run download d18 python script
    print("{0}: download_d18 job is running...".format(datetime.now().strftime('%H:%M:%S')))
    submit_download_d18_job()

    # run processing d18 python script
    print("{0}: process_d18 job is running...".format(datetime.now().strftime('%H:%M:%S')))
    submit_process_d18_job()

    # run staging glue job
    print("{0}: Staging Job running...".format(datetime.now().strftime('%H:%M:%S')))
    submit_staging_gluejob()

    # run d18 glue job
    print("{0}: D18 Glue Job running...".format(datetime.now().strftime('%H:%M:%S')))
    submit_d18_gluejob()

    print("{0}: All D18 completed successfully".format(datetime.now().strftime('%H:%M:%S')))

