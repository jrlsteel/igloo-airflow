import sys
import io
import multiprocessing
from multiprocessing import freeze_support
import timeit

sys.path.append('..')

from common import process_sftp_files as sf
from common import utils as util


class DownloadNRLFiles:

    def __init__(self):
        self.key = 's3_nrl_key'


if __name__ == '__main__':

    s = DownloadNRLFiles()

    start = timeit.default_timer()

    nrl = sf.ProcessSFTPFiles(s.key)   # Enable this to test without multiprocessing
    files = nrl.files

    ######## multiprocessing starts  ##########

    env = util.get_env()
    if env == 'uat':
        n = 12  # number of process to run in parallel
    else:
        n = 24
    print(len(files))
    k = int(len(files) / n)  # get equal no of files for each process
    print(k)
    processes = []
    lv = 0

    for i in range(n+1):
        s = DownloadNRLFiles()
        print(i)
        uv = i * k
        if i == n:
            # print(d18_keys_s3[l:])
            t = multiprocessing.Process(target=nrl.upload_to_s3(), args=(files[lv:]))
        else:
            # print(d18_keys_s3[l:u])
            t = multiprocessing.Process(target=nrl.upload_to_s3(), args=(files[lv:uv]))
        lv = uv

        processes.append(t)

    for p in processes:
        p.start()

    for process in processes:
        process.join()
    ###### multiprocessing Ends #########

    print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')