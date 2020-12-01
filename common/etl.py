from .utils import get_pythonAlias, get_env, get_dir, get_jobID, batch_logging_insert, batch_logging_update
from datetime import datetime
import os
from requests.adapters import HTTPAdapter
from .utils import IglooLogger

DEFAULT_HTTP_TIMEOUT = 5 # seconds


class ETLPipelineWrapperRuntimeException(Exception):
    pass


class ETLPipelineWrapper:
    def __init__(self, job_id, job_name):
        self.pythonAlias = get_pythonAlias()
        self.env = get_env()
        self.dir = get_dir()
        self.job_id = job_id
        self.job_name = job_name
        self.all_jobs_id = get_jobID()
        self.logger = IglooLogger(source=job_name)
    
    def run_all(self):
        pass

    def run(self):
        try:
            batch_logging_insert(self.all_jobs_id, self.job_id, 'all_{}_jobs'.format(self.job_name), os.path.basename(__file__))

            self.logger.in_prod_env('All {} jobs running...'.format(self.job_name))

            self.run_all()

            self.logger.in_prod_env('All {} Jobs completed successfully'.format(self.job_name))

            batch_logging_update(self.all_jobs_id, 'e')
        except Exception as e:
            batch_logging_update(self.all_jobs_id, 'f', str(e))
            raise ETLPipelineWrapperRuntimeException() from e


class ETLPipeline:
    def __init__(self, name):
        self.name = name
        self.logger = IglooLogger(source=name)

    def run(self):
        pass


class TimeoutHTTPAdapter(HTTPAdapter):
    """
    HTTP adapter for use with requests that implements a configurable
    timeout on HTTP connections.
    """
    def __init__(self, *args, **kwargs):
        self.timeout = DEFAULT_HTTP_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)
