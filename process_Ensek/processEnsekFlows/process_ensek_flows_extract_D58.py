from datetime import datetime
import sys
sys.path.append('..')
from common import utils as util
from connections import connect_db as db

def get_files_from_s3(s3_conn, bucket, prefix):
    filenames = []
    print('Reading Filenames')
    more_objects = True
    found_token = False
    while more_objects:
        if found_token:
            response = s3_conn.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                ContinuationToken=found_token)
        else:
            response = s3_conn.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
            )
        # use copy_object or copy_from
        for obj in response["Contents"]:
            filenames.append(obj['Key'])
            print(obj['Key'])
        # Now check there are more objects to list
        if "NextContinuationToken" in response:
            found_token = response["NextContinuationToken"]
            more_objects = True
        else:
            more_objects = False
    return filenames
def strip_data_from_d58_file(s3, bucket, file):
    csv_str = ''
    redshift_date_format = '%Y-%m-%d %H:%M:%S'
    # get the text contents of the file in s3
    file_contents = s3.get_object(Bucket=bucket, Key=file)['Body'].read().decode('utf-8').splitlines(True)
    # All following data extraction is specific to D58 format but can be adapted to other flows
    # D58 file date is the 8th field (index 7) on the first line (index 0). Lines are delimited by | characters
    # datetime.strptime converts a string to datetime given a date format in the string
    # datetime.strftime converts a datetime to string given a date format in the string
    file_date = datetime.strptime(file_contents[0].split('|')[7], '%Y%m%d%H%M%S').strftime(redshift_date_format)
    file_sequence_number = -1
    for line in file_contents:
        linesplit = line.split('|')
        # extract the individual data points necessary from the line types 735 and 137
        if linesplit[0] == '735':
            file_sequence_number = int(linesplit[1])
        if linesplit[0] == '137':
            mpxn = linesplit[3]
            notif_date = datetime.strptime(linesplit[4], '%Y%m%d').strftime(redshift_date_format)
            if linesplit[6] == 'F':
                cot_flag = 'false'
            elif linesplit[6] == 'T':
                cot_flag = 'true'
            else:
                cot_flag = 'null'
            sed = datetime.strptime(linesplit[7], '%Y%m%d').strftime(redshift_date_format)
            # Add the data from the 137 line to the output string
            csv_str += "'{0}',{1},'{2}',{3},'{4}',{5},'{6}'\n".format(file,
                                                                      file_sequence_number,
                                                                      file_date,
                                                                      mpxn,
                                                                      notif_date,
                                                                      cot_flag,
                                                                      sed)
    return csv_str
if __name__ == '__main__':
    dir_s3 = util.get_dir()
    bucket_name = dir_s3['s3_bucket']
    prefix = 'stage1Flows/inbound/subfiles/D0058001/'
    s3 = db.get_S3_Connections_client()
    # Get list of filenames from the d58 folder
    flow_files = get_files_from_s3(s3, bucket_name, prefix)
    # Loop through each file, opening & extracting data into a csv string
    num_files = len(flow_files)
    fnum = 0
    file_str = ''
    for fname in flow_files:
        file_str += strip_data_from_d58_file(s3, bucket_name, fname)
        fnum += 1
        print('{0}/{1} complete'.format(fnum, num_files))
    # Write csv string to file
    with open('all_d58s.csv', 'w+') as f:
        f.write(file_str)
    # Convert the csv lines to sql insert statements (much quicker to import to database than a csv file)
    # This could be replaced by importing csv to dataframe and writing directly into the database,
    # this was just a quick solution
    with open('sql_inserts.sql', 'w+') as sql_file:
        sql_file.write('insert into temp_loss_notification_dates values\n')
        with open('all_d58s.csv', 'r') as csv_file:
            for line in csv_file:
                sql_file.write('({0}),\n'.format(line[:-1]))