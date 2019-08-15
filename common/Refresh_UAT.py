import sys
import subprocess
import awscli as aws

sys.path.append('..')


class SyncS3:
    """
    This script synchronizes the buckets or specific folders in s3
        :param self._source - source s3 path
        :param self._copy_to_env - destination s3 path
    """

    def __init__(self, source, destination):
        self._source_path = source
        self._destination_path = destination

    def process_sync(self):
        if 'prod' in self._destination_path:
            print("error: please check the destination path. PROD keyword detected in destination path")
        else:
            if 'stage2' in self._source_path:
                print("Remove existing files in {0}".format(self._destination_path))
                command = "aws s3 rm {0} --recursive".format(self._destination_path)
                subprocess.run(command, shell=True)

            print("Sync files in s3 from {0} to {1}".format(self._source_path, self._destination_path))
            command = "aws s3 sync {0} {1}".format(self._source_path, self._destination_path)
            subprocess.run(command, shell=True)
            print("completed")

        if 'preprod' in self._destination_path:
            if 'stage2' in self._source_path:
                print("Remove existing files in {0}".format(self._destination_path))
                command = "aws s3 rm {0} --recursive".format(self._destination_path)
                subprocess.run(command, shell=True)

            print("Sync files in s3 from {0} to {1}".format(self._source_path, self._destination_path))
            command = "aws s3 sync {0} {1}".format(self._source_path, self._destination_path)
            subprocess.run(command, shell=True)
            print("completed")


if __name__ == "__main__":

    p = SyncS3("s3://igloo-data-warehouse-prod/stage2/stage2_DirectDebit/", "s3://igloo-data-warehouse-uat/stage2/stage2_DirectDebit1/")
    p.process_sync()
