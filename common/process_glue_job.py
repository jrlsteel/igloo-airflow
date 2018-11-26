import boto3
from time import sleep
import sys

sys.path.append('..')
from connections import connect_db as db


class ProcessGlueJob:
    """
    This is a common routine to run a single glue job/triggers which will accept the below parameters.
    :param glue_job_name: Name of the glue job.
    :param input_files: Optional parameter should be specified if called for "staging job"
    :param trigger_name: Name of the trigger to be submitted

    """
    def __init__(self, job_name='', input_files='', trigger_name =''):
        self.job_name = job_name
        self.input_files = input_files
        self.trigger_name = trigger_name
        self.glue_client = db.get_glue_connection()  # connect to aws glue

    def get_job_status(self, glue, job_run_id):

        coalesce_job = glue.get_job_run(JobName=self.job_name, RunId=job_run_id, PredecessorsIncluded=False)
        cjob_status = coalesce_job['JobRun']['JobRunState']
        cjob_execution_time = coalesce_job['JobRun']['ExecutionTime']

        print("status: " + cjob_status)
        print("execution time: " + str(cjob_execution_time))

        return cjob_status, cjob_execution_time

    def run_glue_job(self):
        try:
            job_status = ''
            job_execution_time = 0

            # Check if already a job is running state
            current_job = self.glue_client.get_job_runs(JobName=self.job_name, MaxResults=1)
            current_job_run_id = current_job['JobRuns'][0]['Id']
            current_job_status = current_job['JobRuns'][0]['JobRunState']

            # Start the job if it is not already  running state ie.
            # NOT in status 'STARTING'|'RUNNING'|'STOPPING'
            if current_job_status.upper() not in ['STARTING', 'RUNNING', 'STOPPING']:
                response = self.glue_client.start_job_run(JobName=self.job_name, Arguments={'--input_files': self.input_files})
                job_run_id = response['JobRunId']
                print("{0} job started... Job Id: {1}".format(self.job_name, job_run_id))
            else:
                print("{0} job already running... Job Id: {1}".format(self.job_name, current_job_run_id))
                job_run_id = current_job_run_id

            # Check Job status for every 3 minutes until it is STOPPED/SUCCEEDED/FAILED/TIMEOUT
            # 'JobRunState': 'STARTING' | 'RUNNING' | 'STOPPING' | 'STOPPED' | 'SUCCEEDED' | 'FAILED' | 'TIMEOUT'
            while job_status.upper() not in ['STOPPED', 'SUCCEEDED', 'FAILED', 'TIMEOUT']:
                sleep(180)
                job_status, job_execution_time = self.get_job_status(self.glue_client, job_run_id)
                if job_status.upper() in ['STOPPED', 'FAILED', 'TIMEOUT']:
                    raise Exception("Job stopped with status {0}. Please check the job id - {1}".format(job_status.upper(), job_run_id))

            job_response = {
                'job_run_id': job_run_id,
                'job_status': job_status,
                'job_execution_time': job_execution_time
            }
            return True

        except:
            raise

    def run_glue_trigger(self):
        try:
            # Start the glue triggers
            response = self.glue_client.start_trigger(Name=self.trigger_name)

            job_response = {
                'trigger_name': response
            }
            return True

        except:
            raise


if __name__ == '__main__':
    s = ProcessGlueJob('process_ref_d18')
    job_gluejob_response = s.run_glue_job()
    job_gluetrigger_response = s.run_glue_trigger()

    print(job_gluejob_response)
    print(job_gluetrigger_response)
