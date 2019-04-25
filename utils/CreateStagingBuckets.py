import timeit
import multiprocessing
import sys

sys.path.append('..')

from connections import connect_db as db


class Processs3():
    def __init__(self):
        self.bucket_name = 'igloo-data-warehouse-prod'
        self.s3 = db.get_S3_Connections_client()
        self.stage1_name = 'stage1'
        self.stage2_name = 'stage2'

    def createfolders(self):

        # self.s3.put_object(Bucket=self.bucket_name, Key=self.stage1_name + '/', Body='')
        self.s3.put_object(Bucket=self.bucket_name, Key=self.stage2_name + '/', Body='')
        # # stage 1 buckets
        # for filenames in self.stage1_files['ensek_files']:
        #     self.s3.put_object(Bucket=self.bucket_name, Key=self.stage1_name + '/' + filenames + '/', Body='')

        # stage 2 buckets
        for filenames in self.stage1_files['ensek_files']:
            self.s3.put_object(Bucket=self.bucket_name, Key=self.stage2_name + '/' + 'stage_' + filenames + '/', Body='')

    def get_keys_from_s3(self, filename, s31):
        """
        This function gets all the d18_keys that needs to be processed.
        :param s3: holds the s3 connection
        :return: list of d18 filenames
        """
        bucket_name = 'igloo-uat-bucket'
        prefix = 'ensek-meterpoints/' + filename
        if filename == 'MeterPointsAttributes':
            prefix = 'ensek-meterpoints/' + 'Attributes'

        suffix = '.csv'
        d18_keys = []
        print('Reading Keys')
        more_objects = True
        found_token = False
        while more_objects:
            if found_token:
                response = s31.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix,
                    ContinuationToken=found_token)
            else:
                response = s31.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix
                    )

            # use copy_object or copy_from
            for obj in response["Contents"]:
                if obj['Key'].endswith(suffix):
                    d18_keys.append(obj['Key'])

                # Now check there is more objects to list
            if "NextContinuationToken" in response:
                found_token = response["NextContinuationToken"]
                more_objects = True
            else:
                more_objects = False
            # print(len(d18_keys))
        # print(filename)
        # print(len(d18_keys))
        return d18_keys

    def copydata(self, keys, s31):
        # for filename in self.stage1_files['ensek_files']:
        #     keys = self.get_keys_from_s3(filename, s31)
        # print(type(keys))
        print('copying data')
        for key in keys:
            copy_source = {
                'Bucket': 'igloo-uat-bucket',
                'Key': key
            }
            key1 = self.stage1_name + '/' + filename + '/' + key.split('/')[-1]
            s31.copy(copy_source, self.bucket_name, key1)
            # print(key1)


if __name__ == '__main__':
    stage1_files = {
        'ensek_files': [
            # 'MeterPoints',
            # 'MeterPointsAttributes',
            'Meters',
            # 'MetersAttributes',
            # 'Registers',
            # 'RegistersAttributes',
            # 'Readings',
            # 'ReadingsInternal',
            # 'AccountStatus',
            # 'RegistrationsElec',
            # 'RegistrationsGas',
            # 'EstimatesElecInternal',
            # 'EstimatesGasInternal',
            # 'TariffHistory',
            # 'TariffHistoryElecStandCharge',
            # 'TariffHistoryElecUnitRates',
            # 'TariffHistoryGasStandCharge',
            # 'TariffHistoryGasUnitRates',
        ],
        'd18_files': [
            'D18/D18BPP',
            'D18/D18PPC'
        ]
    }


    # p.createfolders()

    for filename in stage1_files['ensek_files']:
        print('processing ' + filename)
        s31 = db.get_S3_Connections_client()
        p = Processs3()
        keys = p.get_keys_from_s3(filename, s31)
        # print(type(keys))
        print(len(keys))
        # p.copydata(keys, s31)


        # start Multiprocessing
        n = 10  # number of process to run in parallel
        k = int(len(keys) / n)  # get equal no of files for each process
        print(k)
        processes = []
        lv = 0

        start = timeit.default_timer()

        for i in range(n + 1):
            p1 = Processs3()
            print(i)
            uv = i * k
            if i == n:
                # print(d18_keys_s3[l:])
                t = multiprocessing.Process(target=p1.copydata, args=(keys[lv:], s31))
            else:
                # print(d18_keys_s3[l:u])
                t = multiprocessing.Process(target=p1.copydata, args=(keys[lv:uv], s31))
            lv = uv

            processes.append(t)

        for p in processes:
            p.start()

        for process in processes:
            process.join()
        ####### multiprocessing Ends #########
        print("Copied " + filename)

    print("Process completed in " + str(timeit.default_timer() - start) + ' seconds')
