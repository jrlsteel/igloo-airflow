import sys

sys.path.append('..')
from utils import Refresh_UAT as ref_uat


if __name__ == '__main__':
    source = ""
    destination = ""
    r = ref_uat.SyncS3(source, destination)
    r.process_sync()

