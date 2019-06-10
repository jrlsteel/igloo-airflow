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

    # Donwload NRL files from SFTP
    s = DownloadNRLFiles()
    nrl = sf.ProcessSFTPFiles(s.key)
    files = util.get_files_from_sftp(nrl.sftp_path, search_string='')
    files.remove('Archive')
    if files:
        nrl.upload_to_s3(files)

        # Archive NRL files in SFTP
        archive_path = nrl.sftp_path + '/archive'
        util.archive_files_on_sftp(files, nrl.sftp_path, archive_path)
    else:
        print('No new NRL files available')
