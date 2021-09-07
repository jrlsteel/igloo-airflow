import sys
from datetime import datetime
import timeit
import subprocess

# from cdw.common import process_glue_job as glue
from cdw.common import utils as util


class SmartReadingProcessingJobs:
    def __init__(self):
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.start_smart_all_reporting_jobs_jobid = util.get_jobID()
        self.start_smart_all_jobid = util.get_jobID()
        self.start_smart_all_billing_jobs_jobid = util.get_jobID()
        self.jobName = self.dir["glue_estimated_advance_job_name"]

    def submit_start_smart_all_reporting_jobs_job(self):
        """

        :return: None
        """

        print("{0}: >>>> start_smart_all_reporting_jobs    <<<<".format(datetime.now().strftime("%H:%M:%S")))
        try:
            util.batch_logging_insert(
                self.start_smart_all_reporting_jobs_jobid,
                51,
                "start_smart_all_reporting_jobs",
                "submit_start_smart_all_reporting_jobs_job.py",
            )
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "start_smart_all_reporting_jobs.py"], check=True)
            print(
                "{0}: start_smart_all_reporting_jobs  completed in {1:.2f} seconds".format(
                    datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start)
                )
            )
            util.batch_logging_update(self.start_smart_all_reporting_jobs_jobid, "e")
        except Exception as e:
            util.batch_logging_update(self.start_smart_all_reporting_jobs_jobid, "f", str(e))
            util.batch_logging_update(self.start_smart_all_jobid, "f", str(e))
            print("Error in start_smart_all_reporting_jobs  :- " + str(e))
            sys.exit(1)

    def submit_process_smart_reads_billing_job(self):
        """
        Calls  process_smart_reads_billing.py script
        :return: None
        """

        print("{0}: >>>> process_smart_reads_billing   <<<<".format(datetime.now().strftime("%H:%M:%S")))
        try:
            util.batch_logging_insert(
                self.start_smart_all_billing_jobs_jobid,
                51,
                "Smart Reads Billing pyscript",
                "process_smart_reads_billing.py",
            )
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "process_smart_reads_billing.py"], check=True)
            print(
                "{0}: process_smart_reads_billing  completed in {1:.2f} seconds".format(
                    datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start)
                )
            )
            util.batch_logging_update(self.start_smart_all_billing_jobs_jobid, "e")
        except Exception as e:
            util.batch_logging_update(self.start_smart_all_billing_jobs_jobid, "f", str(e))
            util.batch_logging_update(self.start_smart_all_jobid, "f", str(e))
            print("Error in process_smart_reads_billing  :- " + str(e))
            sys.exit(1)


if __name__ == "__main__":

    s = SmartReadingProcessingJobs()

    util.batch_logging_insert(s.start_smart_all_jobid, 51, "Smart All Jobs", "start_smart_all_jobs.py")

    # #start_smart_all_reporting_jobs_job
    print("{0}:  start_smart_all_reporting_jobs_job running...".format(datetime.now().strftime("%H:%M:%S")))
    s.submit_start_smart_all_reporting_jobs_job()

    # #submit_process_smart_reads_billing_job
    print("{0}:  process_smart_reads_billing_job Running...".format(datetime.now().strftime("%H:%M:%S")))
    s.submit_process_smart_reads_billing_job()

    print("{0}: SmartReadingProcessingJobs completed successfully".format(datetime.now().strftime("%H:%M:%S")))

    util.batch_logging_update(s.start_smart_all_jobid, "e")
