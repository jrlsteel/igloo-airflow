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

    def process_sync(self, env=None):
        if 'prod' in self._destination_path:
            print("error: please check the destination path. PROD keyword detected in destination path")
        else:
            if 'stage2' in self._source_path:
                print("Remove existing files in {0}".format(self._destination_path))
                command = "aws s3 rm {0} --recursive".format(self._destination_path)
                status = subprocess.run(command, shell=True, env=env)
                print(status)

            print("Sync files in s3 from {0} to {1}".format(self._source_path, self._destination_path))
            command = "aws s3 sync {0} {1}".format(self._source_path, self._destination_path)
            status = subprocess.run(command, shell=True, env=env)
            print(status)
            print("completed")

        if 'preprod' in self._destination_path:
            print("Warning you are overwriting input data in Pre-Production ")
            if 'stage2' in self._source_path:
                print("Remove existing files in {0}".format(self._destination_path))
                command = "aws s3 rm {0} --recursive".format(self._destination_path)
                status = subprocess.run(command, shell=True, env=env)
                print(status)

            print("Sync files in s3 from {0} to {1}".format(self._source_path, self._destination_path))
            command = "aws s3 sync {0} {1}".format(self._source_path, self._destination_path)
            status = subprocess.run(command, shell=True, env=env)
            print(status)

            print("completed")

    def copy_sync(self, env=None):
        if 'prod' in self._destination_path:
            print("error: please check the destination path. PROD keyword detected in destination path")
        else:
            print("Remove existing files in {0}".format(self._destination_path))
            command = "aws s3 rm {0} --recursive".format(self._destination_path)
            print(command)
            status = subprocess.run(command, shell=True, env=env)
            print(status)

            print("Sync files in s3 from {0} to {1} --metadata-directive REPLACE".format(self._source_path, self._destination_path))
            command = "aws s3 sync {0} {1} --acl bucket-owner-full-control".format(self._source_path, self._destination_path)
            print(command)
            status = subprocess.run(command, shell=True, env=env)
            print(status)
            print("completed")

        if 'preprod' in self._destination_path:

            print("Remove existing files in {0} --metadata-directive REPLACE".format(self._destination_path))
            command = "aws s3 rm {0} --recursive".format(self._destination_path)
            print(command)
            status = subprocess.run(command, shell=True, env=env)
            print(status)
            print("Sync files in s3 from {0} to {1} --metadata-directive REPLACE".format(self._source_path, self._destination_path))
            command = "aws s3 cp {0} {1} --acl bucket-owner-full-control".format(self._source_path, self._destination_path)
            print(command)
            status = subprocess.run(command, shell=True, env=env)
            print(status)

            print("completed")


if __name__ == "__main__":

    folders = ['TariffHistory', 'TariffHistoryElecStandCharge', 'TariffHistoryElecUnitRates',
               'TariffHistoryGasStandCharge', 'TariffHistoryGasUnitRates']
    for folder in folders:
        src = "s3://igloo-data-warehouse-uat/stage1/{0}/TempNew/".format(folder)
        dest = "s3://igloo-data-warehouse-uat/stage1/{0}/".format(folder)
        p = SyncS3(src, dest)
        p.process_sync()

    # p = SyncS3("s3://igloo-data-warehouse-prod/stage2/stage2_DirectDebit/", "s3://igloo-data-warehouse-uat/stage2/stage2_DirectDebit1/")
    # p.process_sync()
