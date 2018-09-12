

from sqlalchemy import create_engine
import pandas as pd
import pandas_redshift as pr
import ig_config as con
from pathlib import Path
from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkContext
from pyspark.sql.types import *



def extract_meterpoints_data():
    
    # pr.connect_to_redshift(host=con.redshift_config['host'], port=con.redshift_config['port'], user=con.redshift_config['user'], password=con.redshift_config['pwd'], dbname=con.redshift_config['db'])
    # readings_sql = '''with q as 
    #                 (select * , row_number() over(partition by reading_id order by reading_id) row_no from ref_readings)
    #                 select * from q where row_no = 1
    #                 order by q.reading_id,row_no'''
    # ref_readings = pr.redshift_to_pandas(readings_sql)

#   creating spark session 
    spark = SparkSession. \
    builder. \
    master('local[*]'). \
    appName('Process new ensec data'). \
    enableHiveSupport(). \
    getOrCreate()
    
    
#    Adding spark configuration for S3 
    spark._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", con.s3_config['access_key'])
    spark._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", con.s3_config['secret_key'])
    spark._jsc.hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    spark._jsc.hadoopConfiguration().set("spark.sql.parquet.output.committer.class","org.apache.spark.sql.parquet.DirectParquetOutputCommitter")
   
### REDSHIFT ###

# Reading from Redshift - ref_readings_test
    rs_readings = spark.read \
    .format("com.databricks.spark.redshift") \
    .option("url", "jdbc:postgresql://localhost:22/igloosense") \
    .option("user", con.redshift_config['user']) \
    .option("password", con.redshift_config['pwd']) \
    .option("tempdir", "s3n://igloo-uat-bucket/ensek-meterpoints/pysparkTemp/") \
    .option('forward_spark_s3_credentials',True) \
    .option("dbtable", "ref_readings_test") \
    .load() 

    
# Registering redshift data as temp table - readings_s3
    rs_readings.registerTempTable("readings_rs")
    


# Reading from s3 - readings
    s3_df_raw = spark.read \
    .format("com.databricks.spark.csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("s3n://igloo-uat-bucket/ensek-meterpoints/Readings/meter_point_readings_10815_17148.csv") 
    
    
    s3_df = s3_df_raw.withColumn('reading_id', s3_df_raw['reading_id'].cast(LongType()))
    s3_df = s3_df_raw.withColumn('account_id', s3_df_raw['account_id'].cast(LongType())) 
    s3_df = s3_df_raw.withColumn('meter_point_id', s3_df_raw['meter_point_id'].cast(LongType()))
    s3_df = s3_df_raw.withColumn('reading_registerid', s3_df_raw['reading_registerid'].cast(LongType()))
    s3_df = s3_df_raw.withColumn('id', s3_df_raw['id'].cast(LongType()))
    s3_df = s3_df_raw.withColumn('meterPointId', s3_df_raw['meterPointId'].cast(LongType()))
    s3_df = s3_df_raw.withColumn('meterReadingSource', s3_df_raw['meterReadingSource'].cast(StringType()))
    s3_df = s3_df_raw.withColumn('meterpointid', s3_df_raw['meterpointid'].cast(LongType()))
    
    # s3_df.printSchema()

# Registering s3 data as temp table
    s3_df.registerTempTable('readings_s3')

#  Copy from s3 to Redshift
    # redshift_copy = spark.sql("SELECT account_id,meter_point_id,id,datetime,createddate,meterreadingsource,reading_id,reading_registerid,readingtype,reading_value,meterpointid FROM readings_s3") \
    # .write.format("com.databricks.spark.redshift") \
    # .option("url", "jdbc:postgresql://localhost:22/igloosense") \
    # .option("user", con.redshift_config['user']) \
    # .option("password", con.redshift_config['pwd']) \
    # .option("tempdir", "s3n://igloo-uat-bucket/ensek-meterpoints/pysparkTemp/") \
    # .option('forward_spark_s3_credentials',True) \
    # .option("dbtable", "ref_readings_test") \
    # .mode("overwrite") \
    # .save()
    
# Test to count(1) data from s3
    # spark.sql("select count(1) from readings_s3").show()
    # spark.sql("select * from readings_s3 s where s.account_id = 1865 and s.meter_point_id = 2147").show()
   
# ### Insert ###
    readings_insert = spark.sql("select s.account_id, s.meter_point_id, s.id, s.datetime, s.createddate, s.meterreadingsource, s.reading_id, s.reading_registerid, s.readingtype, \
                                s.reading_value, s.meterpointid from readings_s3 s \
                                left outer join readings_rs r  \
                                ON s.reading_id = r.reading_id \
                                where r.reading_id is null")
    
    readings_insert.show()

    readings_insert.coalesce(1).write \
    .format("com.databricks.spark.csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("s3n://igloo-uat-bucket/ensek-meterpoints/altReadings/insert/")

    # ### UPDATE ###
    # readings_update = spark.sql("select s.reading_id,s.account_id,meter_point_id, from readings_s3 s \
    #              inner join readings_rs r  \
    #             ON s.reading_id = r.reading_id \
    #             where (s.meterreadingsource != r.meterreadingsource or s.readingtype != r.readingtype or s.reading_value != r.reading_value")).show()

    # ### DELETE ###
    # readings_delete = spark.sql("select s.reading_id,s.account_id,meter_point_id, from readings_rs r \
    #             left outer join readings_s3 s  \
    #             ON r.reading_id = s.reading_id \
    #             where s.reading_id is null").show()

def main():
   extract_meterpoints_data()



if __name__ == "__main__":
    main()