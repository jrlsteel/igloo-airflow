import boto3
from time import sleep
import sys

sys.path.append('..')
from conf import config as con


def get_job_status(glue, job_run_id):

    coalesce_job = glue.get_job_run(JobName='process_staging_files', RunId=job_run_id, PredecessorsIncluded=False)
    cjob_status = coalesce_job['JobRun']['JobRunState']
    cjob_execution_time = coalesce_job['JobRun']['ExecutionTime']

    print("status: " + cjob_status)
    print("execution time: " + str(cjob_execution_time))

    return cjob_status, cjob_execution_time


def process_staging_job():
    try:
        job_run_id = ''
        job_status = ''
        job_execution_time = 0
        # connect to aws glue
        glue_client = boto3.client(service_name='glue',
                                   region_name='eu-west-1',
                                   aws_access_key_id=con.s3_config['access_key'],
                                   aws_secret_access_key=con.s3_config['secret_key'])

        # Check if already a job is running state
        current_job = glue_client.get_job_runs(JobName='process_staging_files', MaxResults=1)
        current_job_run_id = current_job['JobRuns'][0]['Id']
        current_job_status = current_job['JobRuns'][0]['JobRunState']

        # Start the process_staging_files job if it is not already  running state ie.
        # NOT in status 'STARTING'|'RUNNING'|'STOPPING'
        if current_job_status.upper() not in ['STARTING', 'RUNNING', 'STOPPING']:
            response = glue_client.start_job_run(JobName='process_staging_files')
            job_run_id = response['JobRunId']
            print("staging job started... Job Id: {0}".format(job_run_id))
        else:
            print("staging job already running... Job Id: {0}".format(current_job_run_id))
            job_run_id = current_job_run_id

        # Check Job status for every 3 minutes until it is STOPPED/SUCCEEDED/FAILED/TIMEOUT
        # 'JobRunState': 'STARTING' | 'RUNNING' | 'STOPPING' | 'STOPPED' | 'SUCCEEDED' | 'FAILED' | 'TIMEOUT'
        while job_status.upper() not in ['STOPPED', 'SUCCEEDED', 'FAILED', 'TIMEOUT']:
            sleep(180)
            job_status, job_execution_time = get_job_status(glue_client, job_run_id)
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


if __name__ == '__main__':
    job_response_main = process_staging_job()
    print(job_response_main)
