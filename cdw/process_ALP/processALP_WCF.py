import timeit
import requests
import pandas
from ratelimit import limits, sleep_and_retry
import time
from requests import ConnectionError
import csv
import multiprocessing
from multiprocessing import freeze_support
import datetime
from lxml import etree


import sys
import os


from cdw.conf import config as con
from cdw.common import utils as util
from cdw.connections.connect_db import get_boto_S3_Connections as s3_con


class ALPHistoricalWCF:
    max_calls = con.api_config["max_api_calls"]
    rate = con.api_config["allowed_period_in_secs"]

    def __init__(self):
        self.start_date = datetime.datetime.strptime("2019-01-01", "%Y-%m-%d").date()
        # self.end_date = datetime.datetime.strptime('2018-07-01', '%Y-%m-%d').date()
        self.end_date = datetime.datetime.today().date()
        self.api_url, self.key = util.get_gas_historical_wcf_api_info("gas_historical")
        self.num_days_per_api_calls = 1

    @sleep_and_retry
    @limits(calls=max_calls, period=rate)
    def get_api_response(self, api_url, _end_date, _start_date, wcf_folder):
        """
        get the response for the respective url that is passed as part of this function
        """
        print(api_url)

        session = requests.Session()
        start_time = time.time()
        timeout = con.api_config["connection_timeout"]
        retry_in_secs = con.api_config["retry_in_secs"]

        headers = {"content-type": "text/xml"}
        body = """<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:mipi="http://www.NationalGrid.com/MIPI/">
                       <soap:Header/>
                       <soap:Body>
                          <mipi:GetPublicationDataWM>
                             <!--Optional:-->
                             <mipi:reqObject>
                                <!--Optional:-->
                                <mipi:LatestFlag>Y</mipi:LatestFlag>
                                <!--Optional:-->
                                <mipi:ApplicableForFlag>Y</mipi:ApplicableForFlag>
                                <mipi:ToDate>%s</mipi:ToDate>
                                <mipi:FromDate>%s</mipi:FromDate>
                                <!--Optional:-->
                                <mipi:DateType>gasday</mipi:DateType>
                                <!--Optional:-->
                                <mipi:PublicationObjectNameList>
                                   <!--Zero or more repetitions:-->
                                   <mipi:string>%s</mipi:string>
                                </mipi:PublicationObjectNameList>
                             </mipi:reqObject>
                          </mipi:GetPublicationDataWM>
                       </soap:Body>
                    </soap:Envelope>""" % (
            _end_date,
            _start_date,
            wcf_folder,
        )

        i = 0
        while True:
            try:
                response = session.post(api_url, data=body, headers=headers)

                if response.status_code == 200:
                    if response.content.decode("utf-8") != "":
                        response_xml = response.content
                        return response_xml
                        print(response_xml)
                else:
                    print("Problem Grabbing Data: ", response.status_code)
                    # self.log_error('Response Error: Problem grabbing data', response.status_code)
                    return None
                    break

            except ConnectionError:
                if time.time() > start_time + timeout:
                    print("Unable to Connect after {} seconds of ConnectionErrors".format(timeout))
                    # self.log_error('Unable to Connect after {} seconds of ConnectionErrors'.format(timeout))
                    break
                else:
                    print("Retrying connection in " + str(retry_in_secs) + " seconds" + str(i))
                    # self.log_error('Retrying connection in ' + str(retry_in_secs) + ' seconds' + str(i))

                    time.sleep(retry_in_secs)
            i = i + retry_in_secs

    def extract_wcf_data(self, data_wcf, wcf_folder, k, dir_s3, start_date, end_date):
        ns = {"d": "http://www.NationalGrid.com/MIPI/"}
        data = data_wcf.xpath("//d:CLSMIPIPublicationObjectBE", namespaces=ns)

        for obj in data:
            name = obj.find("d:PublicationObjectName", ns).text
            data = obj.find("d:PublicationObjectData/d:CLSPublicationObjectDataBE", ns)
            applicable_at = data.find("d:ApplicableAt", ns).text
            applicable_for = data.find("d:ApplicableFor", ns).text
            value = float(data.find("d:Value", ns).text)
            # print(name, applicable_at, applicable_for, value)

            alp_wcf_df = pandas.DataFrame(
                [[name, applicable_at, applicable_for, value]],
                columns=["name", "applicable_at", "applicable_for", "value"],
            )

            if alp_wcf_df.empty:
                print(" - has no Weather data")
            else:
                alp_wcf_df += pandas.DataFrame(
                    [[name, applicable_at, applicable_for, value]],
                    columns=["name", "applicable_at", "applicable_for", "value"],
                )
                week_number_iso = start_date.strftime("%V")
                year = start_date.strftime("%Y")
                start_date_string = str(start_date)
                # print(start_date)

                # We specify the column order explicitly when writing the CSV file, as it
                # must match the table schema order in Redshift.
                column_list = util.get_common_info("alp_column_order", "wcf")

                alp_wcf_df_string = alp_wcf_df.to_csv(None, columns=column_list, index=False)
                file_name_alp_wcf = "alp_wcf_historical" + "_" + wcf_folder.strip() + "_" + start_date_string + ".csv"
                k.key = dir_s3["s3_alp_wcf"]["AlpWCF"] + file_name_alp_wcf
                k.set_contents_from_string(alp_wcf_df_string)

    def format_xml_response(self, data):
        data_xml = etree.fromstring(data)
        return data_xml

    def log_error(self, error_msg, error_code=""):
        logs_dir_path = sys.path[0] + "/logs/"
        if not os.path.exists(logs_dir_path):
            os.makedirs(logs_dir_path)
        with open(logs_dir_path + "historical_wcf_log" + time.strftime("%d%m%Y") + ".csv", mode="a") as errorlog:
            employee_writer = csv.writer(errorlog, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
            employee_writer.writerow([error_msg, error_code])

    def processData(self, wcf_folders, k, _dir_s3):
        for wcf_folder in wcf_folders:
            # postcodes[:2]:
            t = con.api_config["total_no_of_calls"]
            msg_ac = "wcf_folders:" + str(wcf_folders)
            # self.log_error(msg_ac, '')
            _start_date = self.start_date
            while _start_date < self.end_date:
                # Logic to fetch date for only 7 days for each call
                _end_date = _start_date + datetime.timedelta(days=1)
                if _end_date > self.end_date:
                    _end_date = self.end_date
                api_url1 = self.api_url.format(wcf_folder)
                print(api_url1)
                api_response = self.get_api_response(api_url1, _end_date, _start_date, wcf_folder)

                if api_response:
                    formatted_xml = self.format_xml_response(api_response)
                    self.extract_wcf_data(formatted_xml, wcf_folder, k, _dir_s3, _start_date, _end_date)
                    _start_date = _end_date


if __name__ == "__main__":

    freeze_support()

    p = ALPHistoricalWCF()

    dir_s3 = util.get_dir()
    bucket_name = dir_s3["s3_bucket"]
    print(bucket_name)
    s3 = s3_con(bucket_name)
    print(s3)
    wcf_folders = []

    wcf_folders = con.test_config["WCFFolders"]
    """Enable this to test for 1 account id"""
    # p.processData(wcf_folders, s3, dir_s3)

    ##### Multiprocessing Starts #########
    #'''Disable if teh above is not commented out'''
    env = util.get_env()
    if env == "uat":
        n = 13  # number of process to run in parallel
    else:
        n = 13

    k = int(len(wcf_folders) / n)  # get equal no of files for each process

    print(len(wcf_folders))
    print(k)

    processes = []
    lv = 0
    start = timeit.default_timer()

    for i in range(n + 1):
        p1 = ALPHistoricalWCF()
        print(i)
        uv = i * k
        if i == n:
            t = multiprocessing.Process(target=p1.processData, args=(wcf_folders[lv:], s3_con(bucket_name), dir_s3))
        else:
            t = multiprocessing.Process(target=p1.processData, args=(wcf_folders[lv:uv], s3_con(bucket_name), dir_s3))
        lv = uv

        processes.append(t)

    for p in processes:
        p.start()
        time.sleep(2)

    for process in processes:
        process.join()
    ####### Multiprocessing Ends #########

    print("Process completed in " + str(timeit.default_timer() - start) + " seconds")
