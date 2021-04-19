"""
All steps for verifying different tasks in batch can be found here.

As many of the steps are similar functionally implementing a class for verification task may be useful

"""


class VerifcationTask:
    def __init__(self, description):
        self.description = description



    def dummy_verification_task(self):
        print("I am a dummy verificaiton task")
    
# To verify a api extract, we would count the number of files added to the s3 bucket today and calculate if its within a sufficient range of live account ids
    def verify_api_extract(self, account_list, output_s3_bucket):
        print("I am a dummy api verification task")

# To verify a staging task we would see if there are 17 new files in the stage 2 s3 bucket
    def verify_staging_task(self, source_s3_bucket, output_s3_bucket):
        print("I am a dummy staging verification task")

# To verify a ref task we would check there is sufficient new data in the relavent ref_tables
    def verify_ref_task(self):
        print("I am a dummy ref_job verification task")



api_extract_verify_meterpoints_object = VerifcationTask(description = 'This task verifies that data was extracted from Ensek API and written to s3' )