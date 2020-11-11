import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append('../..')
# import  common.process_glue_job  as glue
from common import process_glue_job as glue
from common import utils as util
from common import Refresh_UAT as refresh
from conf import config as con


class StartD18MirrorJobs:
    def __init__(self):
        self.process_name = "process D18 "
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.process_d18_extract_jobid = util.get_jobID()
        self.process_d18_staging_jobid = util.get_jobID()
        self.process_d18_reference_jobid = util.get_jobID()
        self.process_d18_job_id = util.get_jobID()
        self.jobName = self.dir['glue_estimated_advance_job_name']

    def submit_process_s3_mirror_job(self, source_input, destination_input):
        """
        Calls the utils/Refresh_UAT.py script which mirrors s3 data from source to destination fdlder
        :return: None
        """

        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
        try:
            util.batch_logging_insert(self.process_d18_job_id, 21,
                                      'process_d18_mirror-' + source_input + '-' + self.env,
                                      'start_processD18_mirror_jobs.py')
            start = timeit.default_timer()
            r = refresh.SyncS3(source_input, destination_input)

            r.process_sync(env={
                'AWS_ACCESS_KEY_ID': con.s3_config['access_key'],
                'AWS_SECRET_ACCESS_KEY': con.s3_config['secret_key']
            })

            util.batch_logging_update(self.process_d18_job_id, 'e')
            print(
                "process_d18_mirror-" + source_input + "-" + self.env + " files completed in {1:.2f} seconds".format(
                    datetime.now().strftime('%H:%M:%S'), float(timeit.default_timer() - start)))
        except Exception as e:
            util.batch_logging_update(self.process_d18_job_id, 'f', str(e))
            util.batch_logging_update(self.process_d18_job_id, 'f', str(e))
            print("Error in process :- " + str(e))
            sys.exit(1)

    def submit_stage2_job(self):
        try:
            util.batch_logging_insert(self.process_d18_staging_jobid, 51, 'process d18 Staging Processing',
                                      'start_ensek_process_d18_jobs.py')
            jobName = self.dir['glue_staging_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env
            obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment,
                                            processJob='proc_d18')
            staging_job_response = obj_stage.run_glue_job()
            if staging_job_response:
                util.batch_logging_update(self.process_d18_staging_jobid, 'e')
                print("{0}: Staging Job Completed successfully for {1}".format(datetime.now().strftime('%H:%M:%S'),
                                                                               self.process_name))
                # return staging_job_response
            else:
                print("Error occurred in {0} Staging Job".format(self.process_name))
                # return staging_job_response
                raise Exception
        except Exception as e:
            util.batch_logging_update(self.process_d18_staging_jobid, 'f', str(e))
            util.batch_logging_update(self.process_d18_job_id, 'f', str(e))
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)


if __name__ == '__main__':

    s = StartD18MirrorJobs()

    util.batch_logging_insert(s.process_d18_job_id, 103, 'all_process_d18_job',
                              'start_ensek_process_d18_mirror_jobs.py')

    if s.env == 'prod':
        # run process D18 Jobs
        print("{0}: process d18 Jobs running...".format(datetime.now().strftime('%H:%M:%S')))
        s.submit_stage2_job()

    elif s.env in ['preprod', 'uat', 'dev']:
        s3_destination_bucket = s.dir['s3_bucket']
        s3_source_bucket = s.dir['s3_source_bucket']

        # run process d18  Jobs Jobs in UAT
        print("{0}:  process d18 JobsJobs running...".format(datetime.now().strftime('%H:%M:%S')))

        print("Ensek process d18 Job Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'),
                                                                             s.process_name))
        source_input = "s3://" + s3_source_bucket + "/stage1/D18/D18Raw/"
        destination_input = "s3://" + s3_destination_bucket + "/stage1/D18/D18Raw/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

    print("{0}: job completed successfully".format(datetime.now().strftime('%H:%M:%S')))

    util.batch_logging_update(s.process_d18_job_id, 'e')

