import sys
from datetime import datetime
import timeit
import subprocess
import os
import traceback

sys.path.append('..')
from common import process_glue_job as glue
from common import utils as util
from common.utils import IglooLogger
from common.etl import ETLPipelineWrapper, ETLPipelineWrapperRuntimeException


class PostcodesETLPipelineWrapper(ETLPipelineWrapper):
    def __init__(self):
        super().__init__(300, 'Postcodes')
        
        self.postcodes_api_extract_job_id = util.get_jobID()
        self.postcodes_api_extract_job_name = 'Postcodes API Extract'

    def run_postcodes_api_extract(self):
        self.logger.in_prod_env(">>>> {} <<<<".format(self.postcodes_api_extract_job_name))
        try:
            util.batch_logging_insert(self.postcodes_api_extract_job_id , self.job_id, self.postcodes_api_extract_job_name, os.path.basename(__file__))
            start = timeit.default_timer()
            subprocess.run([self.pythonAlias, "postcodes_etl.py"], check=True)
            self.logger.in_prod_env("{} completed in {:.2f} seconds".format(self.postcodes_api_extract_job_name,
                                                                       float(timeit.default_timer() - start)))
            util.batch_logging_update(self.postcodes_api_extract_job_id, 'e')
        except Exception as e:
            self.logger.in_prod_env(traceback.format_exc())
            util.batch_logging_update(self.postcodes_api_extract_job_id, 'f', str(e))
            raise ETLPipelineWrapperRuntimeException() from e

    def run_all(self):
        self.run_postcodes_api_extract()


if __name__ == '__main__':
    s = PostcodesETLPipelineWrapper()
    s.run()
