import sys
import multiprocessing
from time import sleep
from multiprocessing import freeze_support
import timeit


from cdw.connections import connect_db as db
from cdw.common import utils as util


class ProcessD18:
    def __init__(self):
        self.dir = util.get_dir()
        self.bucket_name = self.dir["s3_bucket"]
        self.prefix = self.dir["s3_d18_key"]["D18Raw"]
        self.suffix = self.dir["s3_d18_key"]["D18Suffix"]
        self.d018_archive_key = self.dir["s3_d18_key"]["D18Archive"]
        self.upload_key_BPP = self.dir["s3_d18_key"]["D18BPP"]
        self.upload_key_PPC = self.dir["s3_d18_key"]["D18PPC"]

    def process_d18_data(self, d18_keys):

        """
        :param d18_keys: list of D18 files stored in s3
        :return: None

        This function copies the D18 data that is being loaded into D18Raw folder and
            1. Splits the data into BPP and PPC in .csv format and stores back in s3.
            2. Also the data is archived in s3
        """

        try:
            s31 = db.get_S3_Connections_client()

            # loop each file
            for d18key in d18_keys:
                print(d18key)

                obj = s31.get_object(Bucket=self.bucket_name, Key=d18key)

                obj_str = obj["Body"].read().decode("utf-8").splitlines(True)
                # get the filename from the key
                filename = d18key.replace(self.prefix, "")

                fileBPP_csv = filename.replace(self.suffix, "_BPP.csv")
                filePPC_csv = filename.replace(self.suffix, "_PPC.csv")

                # initializing variables
                prev_line_PPC = (
                    prev_line_BPP
                ) = line_ZPD = line_GSP = line_PCL = line_PFL = line_BPP = line_PPC = line_SSC = line_VMR = ""
                # header line for BPP
                line_header_BPP = "ZPD,ST_DATE,ST_CODE,RT_CODE,RUN_NO,GSP_GROUP,GSP,GSP_GROUP_ID,Noon_TEMP_ACT,Noon_TEMP_EFF,TIME_SUNSET,SUNSET_VARIABLE,PCL,PCL_ID,PFL,PFL_ID,BPP,PPC1,PPC2,PPC3,PPC4,PPC5,PPC6,PPC7,PPC8,PPC9,PPC10,PPC11,PPC12,PPC13,PPC14,PPC15,PPC16,PPC17,PPC18,PPC19,PPC20,PPC21,PPC22,PPC23,PPC24,PPC25,PPC26,PPC27,PPC28,PPC29,PPC30,PPC31,PPC32,PPC33,PPC34,PPC35,PPC36,PPC37,PPC38,PPC39,PPC40,PPC41,PPC42,PPC43,PPC44,PPC45,PPC46,PPC47,PPC48,PPC49,PPC50"
                # header line for PPC
                line_header_PPC = "ZPD,ST_DATE,ST_CODE,RT_CODE,RUN_NO,GSP_GROUP,GSP,GSP_GROUP_ID,Noon_TEMP_ACT,Noon_TEMP_EFF,TIME_SUNSET,SUNSET_VARIABLE,PCL,PCL_ID,PFL,PFL_ID,SSC,SS_CONF_ID,VMR,TIME_PATTERN_REGIME,PPC,PPC_1,PRSI_1,PPC_2,PRSI_2,PPC_3,PRSI_3,PPC_4,PRSI_4,PPC_5,PRSI_5,PPC_6,PRSI_6,PPC_7,PRSI_7,PPC_8,PRSI_8,PPC_9,PRSI_9,PPC_10,PRSI_10,PPC_11,PRSI_11,PPC_12,PRSI_12,PPC_13,PRSI_13,PPC_14,PRSI_14,PPC_15,PRSI_15,PPC_16,PRSI_16,PPC_17,PRSI_17,PPC_18,PRSI_18,PPC_19,PRSI_19,PPC_20,PRSI_20,PPC_21,PRSI_21,PPC_22,PRSI_22,PPC_23,PRSI_23,PPC_24,PRSI_24,PPC_25,PRSI_25,PPC_26,PRSI_26,PPC_27,PRSI_27,PPC_28,PRSI_28,PPC_29,PRSI_29,PPC_30,PRSI_30,PPC_31,PRSI_31,PPC_32,PRSI_32,PPC_33,PRSI_33,PPC_34,PRSI_34,PPC_35,PRSI_35,PPC_36,PRSI_36,PPC_37,PRSI_37,PPC_38,PRSI_38,PPC_39,PRSI_39,PPC_40,PRSI_40,PPC_41,PRSI_41,PPC_42,PRSI_42,PPC_43,PRSI_43,PPC_44,PRSI_44,PPC_45,PRSI_45,PPC_46,PRSI_46,PPC_47,PRSI_47,PPC_48,PRSI_48,PPC_49,PRSI_49,PPC_50,PRSI_50"

                # write header
                line_BPP_2 = line_header_BPP + "\n"
                line_PPC_2 = line_header_PPC + "\n"

                # read lines in one file
                for lines in obj_str:
                    if lines.split("|")[0] == "ZPD":
                        line_ZPD = lines.replace("\n", "").replace("|", ",")
                        line_ZPD_st_dt = lines.split("|")[1]
                    elif lines.split("|")[0] == "GSP":
                        line_GSP = lines.replace("\n", "").replace("|", ",")
                    elif lines.split("|")[0] == "PCL":
                        line_PCL = lines.replace("\n", "").replace("|", ",")
                    elif lines.split("|")[0] == "PFL":
                        line_PFL = lines.replace("\n", "").replace("|", ",")
                    elif lines.split("|")[0] == "BPP":
                        line_BPP = lines.replace("\n", "").replace("|", ",")
                    elif lines.split("|")[0] == "SSC":
                        line_SSC = lines.replace("\n", "").replace("|", ",")
                    elif lines.split("|")[0] == "VMR":
                        line_VMR = lines.replace("\n", "").replace("|", ",")
                    elif lines.split("|")[0] == "PPC":
                        line_PPC = lines.replace("\n", "").replace("|", ",")

                    if line_BPP != "" and line_BPP != prev_line_BPP:
                        line_BPP_1 = line_ZPD + line_GSP + line_PCL + line_PFL + line_BPP
                        prev_line_BPP = line_BPP
                        # f_BPP.write(line_BPP_1 + '\n')
                        line_BPP_2 += line_BPP_1 + "\n"
                        # print(line_BPP_1)

                    if line_PPC != "" and line_PPC != prev_line_PPC:
                        line_PPC_1 = line_ZPD + line_GSP + line_PCL + line_PFL + line_SSC + line_VMR + line_PPC
                        prev_line_PPC = line_PPC
                        # f_PPC.write(line_PPC_1 + '\n')
                        line_PPC_2 += line_PPC_1 + "\n"
                        # print(line_PPC_1)

                # upload to s3
                s31.put_object(Bucket=self.bucket_name, Key=self.upload_key_BPP + fileBPP_csv, Body=line_BPP_2)
                s31.put_object(Bucket=self.bucket_name, Key=self.upload_key_PPC + filePPC_csv, Body=line_PPC_2)

                copy_source = {"Bucket": self.bucket_name, "Key": d18key}

                # archive d18
                s31.copy(copy_source, self.bucket_name, self.d018_archive_key + filename)
                # break

        except Exception as e:
            print(" Error :" + str(e))
            sys.exit(1)

    def get_keys_from_s3(self, s3):
        """
        This function gets all the d18_keys that needs to be processed.
        :param s3: holds the s3 connection
        :return: list of d18 filenames
        """
        d18_keys = []
        # get all the files in D018 object
        for obj in s3.list_objects(Bucket=self.bucket_name, Prefix=self.prefix)["Contents"]:
            if obj["Key"].endswith(self.suffix):
                d18_keys.append(obj["Key"])
        return d18_keys


if __name__ == "__main__":

    freeze_support()
    s3 = db.get_S3_Connections_client()
    p = ProcessD18()
    d18_keys_s3 = p.get_keys_from_s3(s3)

    # p.process_d18_data(d18_keys_s3) ##### Enable this to test without multiprocessing
    ######### multiprocessing starts  ##########
    env = util.get_env()
    if env == "uat":
        n = 12  # number of process to run in parallel
    else:
        n = 12
    print(len(d18_keys_s3))
    k = int(len(d18_keys_s3) / n)  # get equal no of files for each process
    print(k)
    processes = []
    lv = 0

    start = timeit.default_timer()

    for i in range(n + 1):
        p1 = ProcessD18()
        print(i)
        uv = i * k
        if i == n:
            # print(d18_keys_s3[l:])
            t = multiprocessing.Process(target=p1.process_d18_data, args=(d18_keys_s3[lv:],))
        else:
            # print(d18_keys_s3[l:u])
            t = multiprocessing.Process(target=p1.process_d18_data, args=(d18_keys_s3[lv:uv],))
        lv = uv

        processes.append(t)

    for p in processes:
        p.start()
        sleep(2)

    for process in processes:
        process.join()
    ####### multiprocessing Ends #########

    print("Process completed in " + str(timeit.default_timer() - start) + " seconds")
