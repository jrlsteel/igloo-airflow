import sys
from time import sleep
from datetime import datetime
import timeit
import subprocess

sys.path.append('..')


def submit_download_d18_job():
    print("{0}: >>>> Downloading D18 files <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        start = timeit.default_timer()
        subprocess.run(["python3", "download_d18.py"], shell=False)
        print("{0}: download_d18 completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
    except Exception as e:
        print("Error in download_d18 process :- " + str(e))
        sys.exit(1)


def submit_process_d18_job():
    print("{0}: >>>> Process D18 files <<<<".format(datetime.now().strftime('%H:%M:%S')))
    try:
        start = timeit.default_timer()
        subprocess.run(["python3", "process_d18.py"], shell=False)
        print("{0}: Process D18 files completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
    except Exception as e:
        print("Error in download_d18 process :- " + str(e))
        sys.exit(1)


def process_d18_jobs():

    # run schema validation job
    print("{0}: download_d18 job is running...".format(datetime.now().strftime('%H:%M:%S')))
    submit_download_d18_job()
    # run processing d18 job
    print("{0}: process_d18 job is running...".format(datetime.now().strftime('%H:%M:%S')))
    submit_process_d18_job()


if __name__ == '__main__':
    process_d18_jobs()

