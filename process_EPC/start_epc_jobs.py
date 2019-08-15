import sys
from datetime import datetime
import timeit
import subprocess

sys.path.append('..')
from common import process_glue_job as glue
from common import utils as util
from common import Refresh_UAT as refresh


class StartEPCJobs:
    def __init__(self):
        self.process_epc_cert_name = "EPC Certificates"
        self.process_epc_reco_name = "EPC Recommendations"
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()
        self.all_jobid = util.get_jobID()
        self.certificates_jobid = util.get_jobID()
        self.recommendations_jobid = util.get_jobID()
        self.certificates_staging_jobid = util.get_jobID()
        self.recommendations_staging_jobid = util.get_jobID()
        self.certificates_ref_jobid = util.get_jobID()
        self.mirror_jobid = util.get_jobID()

    def submit_process_s3_mirror_job(self, source_input, destination_input):
        """
        Calls the utils/Refresh_UAT.py script which mirrors s3 data from source to destination fdlder
        :return: None
        """

        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime('%H:%M:%S'), self.process_name))
        try:

            util.batch_logging_insert(self.mirror_jobid, 28, 'epc_extract_mirror' + source_input + '-' + self.env,
                                      'start_epc_jobs.py')
            start = timeit.default_timer()
            r = refresh.SyncS3(source_input, destination_input)
            r.process_sync()

            util.batch_logging_update(self.mirror_jobid, 'e')
            print("{0}: Process EPC files completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),
                                                                               float(timeit.default_timer() - start)))
        except Exception as e:
            util.batch_logging_update(self.mirror_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in process :- " + str(e))
            sys.exit(1)

    def submit_process_epc_certificates_job(self):
        """
        Calls the epc process_epc_certificates.py script
        :return: None
        """

        print("{0}: >>>> Process EPC Certificates Data <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            util.batch_logging_insert(self.certificates_jobid, 16, 'epc_certificates_extract_pyscript', 'start_epc_jobs.py')
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "process_external_EPC_certificates.py"])
            util.batch_logging_update(self.certificates_jobid, 'e')
            print("{0}: Process EPC Certificates files completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),float(timeit.default_timer() - start)))
        except Exception as e:
            util.batch_logging_update(self.certificates_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in EPC Certificates process :- " + str(e))
            sys.exit(1)

    def submit_process_epc_recommendations_job(self):
        """
        Calls the d18 process_d18.py script to which processes the downloaded data from s3 and extracts the BPP and PPC co efficients.
        :return: None
        """

        print("{0}: >>>> Process EPC Recommendations Data <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            util.batch_logging_insert(self.recommendations_jobid, 17, 'epc_recommendations_extract_pyscript','start_epc_jobs.py')
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "process_external_EPC_recommendations.py"])
            util.batch_logging_update(self.recommendations_jobid, 'e')
            print("{0}: Process EPC Recommendations files completed in {1:.2f} seconds".format(datetime.now().strftime('%H:%M:%S'),float(timeit.default_timer() - start)))
        except Exception as e:
            util.batch_logging_update(self.recommendations_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in EPC Recommendations process :- " + str(e))
            sys.exit(1)

    def submit_epc_certificates_staging_gluejob(self):
        """
                Calls the epc staging job.
                :return: None
                """
        print("{0}: >>>> Staging EPC Certificates Data <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            util.batch_logging_insert(self.certificates_staging_jobid, 18, 'epc_certificates_staging_glue_job','start_epc_jobs.py')
            jobName = self.dir['glue_staging_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env
            obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob='epc-certificates')
            staging_job_response = obj_stage.run_glue_job()
            if staging_job_response:
                util.batch_logging_update(self.certificates_staging_jobid, 'e')
                print("{0}: Staging Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # return staging_job_response
            else:
                print("Error occurred in Staging Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            util.batch_logging_update(self.certificates_staging_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)

    def submit_epc_recommendations_staging_gluejob(self):
        """
                      Calls the epc staging job.
                      :return: None
                      """
        print("{0}: >>>> Staging EPC Recommendations Data <<<<".format(datetime.now().strftime('%H:%M:%S')))
        try:
            util.batch_logging_insert(self.recommendations_staging_jobid, 19, 'epc_recommendations_staging_glue_job','start_epc_jobs.py')
            jobName = self.dir['glue_staging_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env

            obj_stage = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob='epc-recommendations')
            staging_job_response = obj_stage.run_glue_job()
            if staging_job_response:
                util.batch_logging_update(self.recommendations_staging_jobid, 'e')
                print("{0}: Staging Job Completed successfully".format(datetime.now().strftime('%H:%M:%S')))
                # return staging_job_response
            else:
                print("{0}: Error occurred in Staging Job".format(datetime.now().strftime('%H:%M:%S')))
                # return staging_job_response
                raise Exception
        except Exception as e:
            util.batch_logging_update(self.recommendations_staging_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)


    def submit_ref_epc_certificates_gluejob(self):
        try:
            jobName = self.dir['glue_epc_job_name']
            s3_bucket = self.dir['s3_bucket']
            environment = self.env
            util.batch_logging_insert(self.certificates_ref_jobid, 20, 'epc_certificates_ref_glue_job','start_epc_jobs.py')
            obj = glue.ProcessGlueJob(job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob='epc_certificates')
            job_response = obj.run_glue_job()
            if job_response:
                util.batch_logging_update(self.certificates_ref_jobid, 'e')
                print("{0}: Ref Glue Job Completed successfully for {1}".format(datetime.now().strftime('%H:%M:%S'), self.process_epc_cert_name))
                # return staging_job_response
            else:
                print("{0}: Error occurred in {1} Job".format(datetime.now().strftime('%H:%M:%S'), self.process_epc_cert_name))
                # return staging_job_response
                raise Exception
        except Exception as e:
            util.batch_logging_update(self.certificates_ref_jobid, 'f', str(e))
            util.batch_logging_update(self.all_jobid, 'f', str(e))
            print("Error in Ref Glue Job :- " + str(e))
            sys.exit(1)


if __name__ == '__main__':

    s = StartEPCJobs()

    util.batch_logging_insert(s.all_jobid, 104, 'all_epc_jobs', 'start_epc_jobs.py')

    if s.env == 'prod':
        # run processing epc certificates python script
        print("{0}: {1} job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_epc_cert_name))
        s.submit_process_epc_certificates_job()

        # run processing epc recommendations python script
        print("{0}: {1} job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_epc_cert_name))
        s.submit_process_epc_recommendations_job()

    else:
        # # run EPC Certificates Mirror Job
        print("EPC Certificates  Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/EPC/EPCCertificates/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/EPC/EPCCertificates/"
        s.submit_process_s3_mirror_job(source_input, destination_input)

        # # run EPC Recommendations Mirror Job
        print("EPC Certificates  Mirror  job is running...".format(datetime.now().strftime('%H:%M:%S'), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/EPC/EPCRecommendations/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/EPC/EPCRecommendations/"
        s.submit_process_s3_mirror_job(source_input, destination_input)


    # run staging glue job epc  certificates
    print("{0}: Staging Job running for {1}...".format(datetime.now().strftime('%H:%M:%S'), s.process_epc_cert_name))
    s.submit_epc_certificates_staging_gluejob()

    # # # run staging glue job recommendations
    print("{0}: Staging Job running for {1}...".format(datetime.now().strftime('%H:%M:%S'), s.process_epc_reco_name))
    s.submit_epc_recommendations_staging_gluejob()

    # run EPC Certificates glue job
    print("{0}: Glue Job running for {1}...".format(datetime.now().strftime('%H:%M:%S'), s.process_epc_cert_name))
    s.submit_ref_epc_certificates_gluejob()

    print("{0}: All D18 completed successfully".format(datetime.now().strftime('%H:%M:%S')))

    util.batch_logging_update(s.all_jobid, 'e')

