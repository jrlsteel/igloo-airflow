import sys
from datetime import datetime
import timeit
import subprocess


from cdw.common import process_glue_job as glue
from cdw.common import utils as util
from cdw.common import Refresh_UAT as refresh
from cdw.process_calculated_steps import start_calculated_steps_jobs as sj


class ALP:
    def __init__(self):
        self.process_name = "ALP WCF CV Historical EAC_AQ CONS_ACCU "
        self.pythonAlias = util.get_pythonAlias()
        self.env = util.get_env()
        self.dir = util.get_dir()

        self.all_jobid = util.get_jobID()

        self.alp_wcf_jobid = util.get_jobID()
        self.alp_cv_jobid = util.get_jobID()
        self.alp_wcf_staging_jobid = util.get_jobID()
        self.alp_cv_staging_jobid = util.get_jobID()
        self.alp_ref_jobid = util.get_jobID()

    def submit_process_alp_wcf_job(self):
        """
        Calls the processALP_WCF.py script which processes historical data from gas national grid for the past 1 year
        :return: None
        """

        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime("%H:%M:%S"), self.process_name))
        try:
            util.batch_logging_insert(
                self.alp_wcf_jobid, 31, "alp_wcf_extract_pyscript", "start_alp_historical_jobs.py"
            )
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "processALP_WCF.py"], check=True)
            util.batch_logging_update(self.alp_wcf_jobid, "e")

            print(
                "{0}: Processing of {2} Data completed in {1:.2f} seconds".format(
                    datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start), self.process_name
                )
            )
        except Exception as e:
            util.batch_logging_update(self.alp_wcf_jobid, "f", str(e))
            util.batch_logging_update(self.all_jobid, "f", str(e))
            print("Error in process :- " + str(e))
            sys.exit(1)

    def submit_process_s3_mirror_job(self, source_input, destination_input):
        """
        Calls the utils/Refresh_UAT.py script which mirrors s3 data from source to destination folder
        :return: None
        """

        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime("%H:%M:%S"), self.process_name))
        try:
            util.batch_logging_insert(
                self.alp_wcf_jobid,
                31,
                "alp_wcf_extract_mirror" + source_input + "-" + self.env,
                "start_alp_historical_jobs.py",
            )
            start = timeit.default_timer()
            r = refresh.SyncS3(source_input, destination_input)
            r.process_sync()

            util.batch_logging_update(self.alp_wcf_jobid, "e")

            print(
                "{0}: Processing of {2} Data completed in {1:.2f} seconds".format(
                    datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start), self.process_name
                )
            )
        except Exception as e:
            util.batch_logging_update(self.alp_wcf_jobid, "f", str(e))
            util.batch_logging_update(self.all_jobid, "f", str(e))
            print("Error in process :- " + str(e))
            sys.exit(1)

    def submit_process_alp_cv_job(self):
        """
        Calls the processALP_WCF.py script which processes historical data from gas national grid for the past 1 year
        :return: None
        """

        print("{0}: >>>> Process {1}<<<<".format(datetime.now().strftime("%H:%M:%S"), self.process_name))
        try:
            util.batch_logging_insert(self.alp_cv_jobid, 32, "alp_cv_extract_pyscript", "start_alp_historical_jobs.py")
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "processALP_CV.py"], check=True)
            util.batch_logging_update(self.alp_cv_jobid, "e")

            print(
                "{0}: Processing of {2} Data completed in {1:.2f} seconds".format(
                    datetime.now().strftime("%H:%M:%S"), float(timeit.default_timer() - start), self.process_name
                )
            )
        except Exception as e:
            util.batch_logging_update(self.alp_cv_jobid, "f", str(e))
            util.batch_logging_update(self.all_jobid, "f", str(e))
            print("Error in process :- " + str(e))
            sys.exit(1)

    def submit_alp_wcf_staging_gluejob(self):
        try:
            util.batch_logging_insert(
                self.alp_wcf_staging_jobid, 33, "alp_wcf_staging_glue_job", "start_alp_historical_jobs.py"
            )
            jobName = self.dir["glue_staging_job_name"]
            s3_bucket = self.dir["s3_bucket"]
            environment = self.env

            obj_stage = glue.ProcessGlueJob(
                job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob="alp-wcf"
            )
            staging_job_response = obj_stage.run_glue_job()
            if staging_job_response:
                util.batch_logging_update(self.alp_wcf_staging_jobid, "e")
                print(
                    "{0}: Staging Job Completed successfully for {1}".format(
                        datetime.now().strftime("%H:%M:%S"), self.process_name
                    )
                )
                # return staging_job_response
            else:
                print("Error occurred in {0} Staging Job".format(self.process_name))
                # return staging_job_response
                raise Exception

        except Exception as e:
            util.batch_logging_update(self.alp_wcf_staging_jobid, "f", str(e))
            util.batch_logging_update(self.all_jobid, "f", str(e))
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)

    def submit_alp_cv_staging_gluejob(self):
        try:
            util.batch_logging_insert(
                self.alp_cv_staging_jobid, 34, "alp_cv_staging_glue_job", "start_alp_historical_jobs.py"
            )
            jobName = self.dir["glue_staging_job_name"]
            s3_bucket = self.dir["s3_bucket"]
            environment = self.env

            obj_stage = glue.ProcessGlueJob(
                job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob="alp-cv"
            )
            staging_job_response = obj_stage.run_glue_job()
            if staging_job_response:
                util.batch_logging_update(self.alp_cv_staging_jobid, "e")
                print(
                    "{0}: Staging Job Completed successfully for {1}".format(
                        datetime.now().strftime("%H:%M:%S"), self.process_name
                    )
                )
            else:
                print("Error occurred in {0} Staging Job".format(self.process_name))
                # return staging_job_response
                raise Exception

        except Exception as e:
            util.batch_logging_update(self.alp_cv_staging_jobid, "f", str(e))
            util.batch_logging_update(self.all_jobid, "f", str(e))
            print("Error in Staging Job :- " + str(e))
            sys.exit(1)

    def submit_alp_gluejob(self):
        try:
            util.batch_logging_insert(self.alp_ref_jobid, 35, "alp_cv_ref_glue_job", "start_alp_historical_jobs.py")

            jobName = self.dir["glue_alp_job_name"]
            s3_bucket = self.dir["s3_bucket"]
            environment = self.env

            obj_alp = glue.ProcessGlueJob(
                job_name=jobName, s3_bucket=s3_bucket, environment=environment, processJob="alp"
            )
            alp_job_response = obj_alp.run_glue_job()
            if alp_job_response:
                util.batch_logging_update(self.alp_ref_jobid, "e")
                print("{0}: ALP Job Completed successfully".format(datetime.now().strftime("%H:%M:%S")))
                # return staging_job_response
            else:
                print("Error occurred in ALP Job")
                # return staging_job_response
                raise Exception
        except Exception as e:
            util.batch_logging_update(self.alp_ref_jobid, "f", str(e))
            util.batch_logging_update(self.all_jobid, "f", str(e))
            print("Error in ALP Job :- " + str(e))
            sys.exit(1)


if __name__ == "__main__":

    s = ALP()

    util.batch_logging_insert(s.all_jobid, 105, "all_alp_jobs", "start_alp_jobs.py")
    if s.env == "prod":
        # # run processing alp wcf script
        print("{0}: {1} job is running...".format(datetime.now().strftime("%H:%M:%S"), s.process_name))
        s.submit_process_alp_wcf_job()
    else:
        # # run processing alp wcf script
        print("{0}: {1} job is running...".format(datetime.now().strftime("%H:%M:%S"), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/ALP/AlpWCF/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/ALP/AlpWCF/"
        s.submit_process_s3_mirror_job(source_input, destination_input)
    #
    # # run alp cv python script
    if s.env == "prod":
        # # run processing alp wcf script
        print("{0}: {1} job is running...".format(datetime.now().strftime("%H:%M:%S"), s.process_name))
        s.submit_process_alp_cv_job()
    else:
        # # run processing alp wcf script
        print("{0}: {1} job is running...".format(datetime.now().strftime("%H:%M:%S"), s.process_name))
        source_input = "s3://igloo-data-warehouse-prod/stage1/ALP/AlpCV/"
        destination_input = "s3://igloo-data-warehouse-" + s.env + "/stage1/ALP/AlpCV/"
        s.submit_process_s3_mirror_job(source_input, destination_input)
    #
    # # run alp wcf staging glue job
    print("{0}: Staging Job running for {1}...".format(datetime.now().strftime("%H:%M:%S"), s.process_name))
    s.submit_alp_wcf_staging_gluejob()
    #
    # # run alp cv staging glue job
    print("{0}: Staging Job running for {1}...".format(datetime.now().strftime("%H:%M:%S"), s.process_name))
    s.submit_alp_cv_staging_gluejob()
    #
    # # run reference alp glue job
    print("{0}: ALP Glue Job running...".format(datetime.now().strftime("%H:%M:%S")))
    s.submit_alp_gluejob()

    print("{0}: All {1} completed successfully".format(datetime.now().strftime("%H:%M:%S"), s.process_name))

    # run consumption accuracy job
    print("{0}: All Calcs..".format(datetime.now().strftime("%H:%M:%S")))
    sj_obj = sj.CalcSteps()
    sj_obj.startCalcJobs()

    util.batch_logging_update(s.all_jobid, "e")
