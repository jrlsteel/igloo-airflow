import datetime
import sys
from time import sleep

import cdw.common

from cdw.connections import connect_db as db


class ProcessGlueJob:
    """
    This is a common routine to run a single glue job/trigger which will accept the below parameters.
    This program will not EXIT until the job finishes. Checks and Returns the status for every 180 seconds(3 minutes).
    :param job_name: Name of the glue job.
    :param input_files: Optional parameter should be specified if called for "staging job"
    :param trigger_name: Name of the trigger to be submitted

    """

    def __init__(self, job_name="", s3_bucket="", environment="", trigger_name="", processJob=""):
        self.job_name = job_name
        self.process_job = processJob
        self.trigger_name = trigger_name
        self.s3_bucket = s3_bucket
        self.environment = environment
        self.glue_client = db.get_glue_connection()  # connect to aws glue

    def get_job_status(self, glue, job_run_id):

        coalesce_job = glue.get_job_run(JobName=self.job_name, RunId=job_run_id, PredecessorsIncluded=False)
        cjob_status = coalesce_job["JobRun"]["JobRunState"]
        cjob_execution_time = coalesce_job["JobRun"]["ExecutionTime"]
        cjob_running_name = coalesce_job["JobRun"]["Arguments"]["--process_job"]

        print("status: " + cjob_status)
        print("execution time: " + str(cjob_execution_time))

        return cjob_status, cjob_execution_time, cjob_running_name

    def run_glue_job(self):
        job_status = ""

        # Check if already a job is running state
        current_job = self.glue_client.get_job_runs(JobName=self.job_name, MaxResults=1)
        current_job_run_id = current_job["JobRuns"][0]["Id"]
        current_job_status = current_job["JobRuns"][0]["JobRunState"]
        current_job_name = current_job["JobRuns"][0]["Arguments"]["--process_job"]

        # Start the job if it is not already  running state ie.
        # NOT in status 'STARTING'|'RUNNING'|'STOPPING'
        if current_job_status.upper() not in ["STARTING", "RUNNING", "STOPPING"]:
            sleep(120)  # sleep to avoid concurrent execution
            response = self.start_glue_job()

            job_run_id = response["JobRunId"]
            print("{0} job started for {1}... Job Id: {2}".format(self.job_name, self.process_job, job_run_id))

        else:
            print(
                "{0} job already running for {1}... Job Id: {2}".format(
                    self.job_name, current_job_name, current_job_run_id
                )
            )
            job_run_id = current_job_run_id

        # Check Job status for every 3 minutes until it is STOPPED/SUCCEEDED/FAILED/TIMEOUT
        # 'JobRunState': 'STARTING' | 'RUNNING' | 'STOPPING' | 'STOPPED' | 'SUCCEEDED' | 'FAILED' | 'TIMEOUT'
        job_completed_state = ["STOPPED", "SUCCEEDED", "FAILED", "TIMEOUT"]
        exception_state = ["STOPPED", "FAILED", "TIMEOUT"]

        while job_status.upper() not in job_completed_state:
            sleep(120)
            job_status, job_execution_time, running_job = self.get_job_status(self.glue_client, job_run_id)
            if job_status.upper() in exception_state and self.process_job == running_job:
                raise Exception(
                    "Job stopped with status {0}. Please check the job id - {1}".format(job_status.upper(), job_run_id)
                )

            if job_status.upper() in job_completed_state and self.process_job != running_job:
                sleep(120)  # sleep to avoid concurrent execution
                response = self.start_glue_job()
                job_run_id = response["JobRunId"]
                job_status, job_execution_time, running_job = self.get_job_status(self.glue_client, job_run_id)
                print("{0} job started for {1}... Job Id: {2}".format(self.job_name, running_job, job_run_id))
        return True

    def start_glue_job(self):
        response = self.glue_client.start_job_run(
            JobName=self.job_name,
            Arguments={
                "--process_job": self.process_job,
                "--s3_bucket": self.s3_bucket,
                "--environment": self.environment,
            },
        )
        return response

    def run_glue_trigger(self):
        try:
            # Start the glue triggers
            response = self.glue_client.start_trigger(Name=self.trigger_name)

            job_response = {"trigger_name": response}
            return True

        except:
            raise


def run_glue_job_await_completion(job_name, process_name):
    directory = cdw.common.utils.get_dir()
    s3_bucket = directory["s3_bucket"]
    environment = cdw.common.utils.get_env()

    try:
        print("job_name-- ", job_name)
        print("s3_bucket-- ", s3_bucket)
        print("environment-- ", environment)
        print("process_name-- ", process_name)
        glue_job = ProcessGlueJob(
            job_name=job_name,
            s3_bucket=s3_bucket,
            environment=environment,
            processJob=process_name,
        )
        glue_job.run_glue_job()

        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        print(f"{timestamp}: Glue job completed successfully: job_name={job_name}, process_name={process_name}")
    except:
        print(f"Error running glue job: job_name={job_name}, process_name={process_name}")
        print(sys.exc_info()[0])
        raise


if __name__ == "__main__":
    s = ProcessGlueJob("process_ref_d18")
    s.run_glue_job()
    job_gluetrigger_response = s.run_glue_trigger()

    print(job_gluetrigger_response)
