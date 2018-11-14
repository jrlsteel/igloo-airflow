import sys

sys.path.append('..')

from conf import config as con
from connections import connect_db as db


def process_d18_data():
    try:
        s3 = db.get_S3_Connections_resources()
        filename = '100444.flw'
        bucket = 'igloo-uat-bucket'
        key = 'iglo-d18/20181012D18/' + filename
        obj = s3.Object(bucket, key)
        obj_str = obj.get()['Body'].read().decode('utf-8').splitlines(True)
        # s3.meta.client.download_file(bucket, key, 'd18_001.txt')
        # print(obj_str)
        line_ZHV = line_GSP = line_PCL = line_BPP = line_PPC = line_SSC = line_VMR = ''
        line_pcl_1_2 = False
        prev_line_GSP = ''
        f = open('processed_d18_file.txt', "w")

        # read lines in one file
        for lines in obj_str[:1100]:
            if lines.split('|')[0] == 'ZHV':
                line_ZHV = lines.replace('\n', '')
            elif lines.split('|')[0] == 'GSP':
                line_GSP = lines.replace('\n', '')
            elif lines.split('|')[0] == 'PCL':
                if int(lines.split('|')[1]) in (1, 2):
                    line_pcl_1_2 = True
                    line_PCL = lines.replace('\n', '')
                else:
                    line_pcl_1_2 = False
            if line_pcl_1_2:
                if lines.split('|')[0] == 'BPP':
                    line_BPP = lines.replace('\n', '')
                elif lines.split('|')[0] == 'SSC':
                    line_SSC = lines.replace('\n', '')
                elif lines.split('|')[0] == 'VMR':
                    line_VMR = lines.replace('\n', '')
                elif lines.split('|')[0] == 'PPC':
                    line_PPC = lines.replace('\n', '')

            line_head = line_ZHV + line_GSP + line_PCL

            if prev_line_GSP != line_GSP and line_BPP != '':
                prev_line_GSP = line_GSP
                line_BPP_1 = line_head + line_BPP
                f.write(line_BPP_1 + '\n')
                print(line_BPP_1)

            if line_PPC != '':
                line_PPC_1 = line_head + line_SSC + line_VMR + line_PPC
                f.write(line_PPC_1 + '\n')
                print(line_PPC_1)
    except:
        raise
    finally:
        f.close()


if __name__ == "__main__":
    process_d18_data()
