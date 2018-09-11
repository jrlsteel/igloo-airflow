

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
    master('local'). \
    appName('Getting Started'). \
    enableHiveSupport(). \
    getOrCreate()
    
    
#    Adding spark configuration for S3 
    spark._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", con.s3_config['access_key'])
    spark._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", con.s3_config['secret_key'])
    spark._jsc.hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    '''
### REDSHIFT ###

    #Reading from Redshift
    rs_readings = spark.read \
    .format("com.databricks.spark.redshift") \
    .option("url", "jdbc:postgresql://localhost:22/igloosense") \
    .option("user", con.redshift_config['user']) \
    .option("password", con.redshift_config['pwd']) \
    .option("tempdir", "s3n://igloo-uat-bucket/ensek-meterpoints/pysparkTemp/") \
    .option('forward_spark_s3_credentials',True) \
    .option("dbtable", "ref_readings") \
    .load() 

    
    # Registering redshift data as temp table
    rs_readings.registerTempTable("readings_rs")
    '''


    # Reading from s3
    s3_df_raw = spark.read \
    .format("com.databricks.spark.csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("s3n://igloo-uat-bucket/ensek-meterpoints/Readings/*.csv") 
    
    
    s3_df = s3_df_raw.withColumn('reading_id', s3_df_raw['reading_id'].cast(LongType()))
    s3_df = s3_df_raw.withColumn('account_id', s3_df_raw['account_id'].cast(LongType())) 
    s3_df = s3_df_raw.withColumn('meter_point_id', s3_df_raw['meter_point_id'].cast(LongType()))
    s3_df = s3_df_raw.withColumn('reading_registerid', s3_df_raw['reading_registerid'].cast(LongType()))
    s3_df = s3_df_raw.withColumn('id', s3_df_raw['id'].cast(LongType()))
    s3_df = s3_df_raw.withColumn('meterPointId', s3_df_raw['meterPointId'].cast(LongType()))
    s3_df = s3_df_raw.withColumn('meterReadingSource', s3_df_raw['meterReadingSource'].cast(StringType()))
    s3_df = s3_df_raw.withColumn('meterpointid', s3_df_raw['meterpointid'].cast(LongType()))
    
    s3_df.printSchema()

    # Registering s3 data as temp table
    s3_df.registerTempTable('readings_s3')

# Copy from s3 to Redshift
    spark.sql("SELECT account_id,meter_point_id,id,datetime,createddate,meterreadingsource,reading_id,reading_registerid,readingtype,reading_value,meterpointid FROM reading_s3") \
    .write.format("com.databricks.spark.redshift") \
    .option("url", "jdbc:postgresql://localhost:22/igloosense") \
    .option("user", con.redshift_config['user']) \
    .option("password", con.redshift_config['pwd']) \
    .option("tempdir", "s3n://igloo-uat-bucket/ensek-meterpoints/pysparkTemp/") \
    .option('forward_spark_s3_credentials',True) \
    .option("dbtable", "ref_readings") \
    .mode("overwrite") \
    .save()
    

    # .mode(SaveMode.Overwrite) \
    # spark.sql("select * from readings_s3 s where s.account_id = 1865 and s.meter_point_id = 2147").show()



    
    # ### Insert ###
    # spark.sql("select s.reading_id,s.account_id,meter_point_id, from readings_s3 s \
    #             left outer join readings_rs r  \
    #             ON s.reading_id = r.reading_id \
    #             where r.reading_id is null").show()

    

def main():
   extract_meterpoints_data()



if __name__ == "__main__":
    main()