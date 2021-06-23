import sys
import timeit

sys.path.append("..")

from common import process_sftp_files as sf
from common import utils as util


class DownloadNosiFiles:
    def __init__(self):
        self.key = "s3_nosi_key"


if __name__ == "__main__":

    s = DownloadNosiFiles()

    start = timeit.default_timer()

    nosi = sf.ProcessSFTPFiles(s.key)
    files = util.get_files_from_sftp(nosi.sftp_path, search_string="")
    files.remove("Archive")
    if files:
        nosi.upload_to_s3(files, "|")

        # Archive NRL files in SFTP
        archive_path = nosi.sftp_path + "/archive"
        util.archive_files_on_sftp(files, nosi.sftp_path, archive_path)
    else:
        print("No new NOSI files available")
