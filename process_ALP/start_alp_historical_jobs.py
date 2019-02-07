import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append('..')
from common import process_glue_job as glue
from common import utils as util


class ALP:
    def __init__(self):
        self.process_name = "ALP WCF CV Historical"
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()

    def submit_process_alp_wcf_job(self):
        """
        Calls the processALP_WCF.py script which processes historical data from gas national grid for the past 1 year
        :return: None
        """

        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
        try:
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "processALP_WCF.py"])
            print("{0}: Processing of {2} Data completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start), self.process_name))
        except Exception as e:
            print("Error in process :- " + str(e))
            sys.exit(1)

    def submit_process_alp_cv_job(self):
        """
        Calls the processALP_WCF.py script which processes historical data from gas national grid for the past 1 year
        :return: None
        """

        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
        try:
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "processALP_CV.py"])
            print("{0}: Processing of {2} Data completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start), self.process_name))
        except Exception as e:
            print("Error in process :- " + str(e))
            sys.exit(1)

    def submit_alp_wcf_staging_gluejob(self):
        try:
            jobName = self.dir['glue_staging_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob='alp-wcf')
            staging_job_response = obj_stage.run_glue_job()
            if staging_job_response:
                print("{0}: Staging Job Completed successfully for {1}".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
                # return staging_job_response
            else:
                print("Error occurred in {0} Staging Job".format(self.process_name))
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)

    def submit_alp_cv_staging_gluejob(self):
        try:
            jobName = self.dir['glue_staging_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment,
                                            processJob='alp-cv')
            staging_job_response = obj_stage.run_glue_job()
            if staging_job_response:
                print("{0}: Staging Job Completed successfully for {1}".format(datetime.now().strftime('%H:%M:%S'),
                                                                               self.process_name))
                # return staging_job_response
            else:
                print("Error occurred in {0} Staging Job".format(self.process_name))
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)

    def submit_alp_gluejob(self):
        try:
            jobName = self.dir['glue_alp_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_d18 = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment,
                                          processJob='alp')
            d18_job_response = obj_d18.run_glue_job()
            if d18_job_response:
                print("{0}: ALP Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # return staging_job_response
            else:
                print("Error occurred in ALP Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in ALP Job :- " + str(e))
            sys.exit(1)

    def submit_eac_aq_gluejob(self):
        try:
            jobName = self.dir['glue_eac_aq_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_d18 = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment,
                                          processJob='eac_aq')
            d18_job_response = obj_d18.run_glue_job()
            if d18_job_response:
                print("{0}: EAC and AQ Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # return staging_job_response
            else:
                print("Error occurred in EAC and AQ Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            print("Error in EAC and AQ Job :- " + str(e))
            sys.exit(1)


if __name__ == '__main__':

    s = ALP()

    # run processing alp wcf script
    print("{0}: {1} job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
    s.submit_process_alp_wcf_job()

    # run alp cv python script
    print("{0}: {1} job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
    s.submit_process_alp_cv_job()

    # run alp wcf staging glue job
    print("{0}: Staging Job running for {1}...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
    s.submit_alp_wcf_staging_gluejob()

    # run alp cv staging glue job
    print("{0}: Staging Job running for {1}...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
    s.submit_alp_cv_staging_gluejob()

    # run reference alp glue job
    print("{0}: ALP Glue Job running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_alp_gluejob()

    # run eac and aq calculation job
    print("{0}: EAC and AQ Glue Job running...".format(datetime.now().strftime('%H:%M:%S')))
    s.submit_eac_aq_gluejob()

    print("{0}: All {1} completed successfully".format(datetime.now().strftime('%H:%M:%S'), s.process_name))

