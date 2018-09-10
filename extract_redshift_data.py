

from sqlalchemy import create_engine
import pandas as pd
import pandas_redshift as pr
import ig_config as con
from pathlib import Path
from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkContext
from pyspark.sql.types import *



def extract_meterpoints_data():
    
    pr.connect_to_redshift(host=con.redshift_config['host'], port=con.redshift_config['port'], user=con.redshift_config['user'], password=con.redshift_config['pwd'], dbname=con.redshift_config['db'])
    readings_sql = '''with q as 
                    (select * , row_number() over(partition by reading_id order by reading_id) row_no from ref_readings)
                    select * from q where row_no = 1
                    order by q.reading_id,row_no'''
    ref_readings = pr.redshift_to_pandas(readings_sql)
    # ref_meterpoints = pr.redshift_to_pandas("SELECT * FROM public.ref_meterpoints limit 1000")
    # ref_meters = pr.redshift_to_pandas("SELECT * FROM public.ref_meters limit 1000")
    # ref_metersattributes = pr.redshift_to_pandas("SELECT * FROM public.ref_metersattributes limit 1000")
    # ref_registers = pr.redshift_to_pandas("SELECT * FROM public.ref_registers limit 1000")
    # ref_registersattributes = pr.redshift_to_pandas("SELECT * FROM public.ref_registersattributes limit 1000")
    # ref_attributes = pr.redshift_to_pandas("SELECT * FROM public.ref_attributes limit 1000")

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

    # sqlContext = SQLContext(spark)
    # ref_readings = sqlContext.read \
    # .format("com.databricks.spark.redshift") \
    # .option("url", con.redshift_config['host']) \
    # .option("dbtable", "ref_readings") \
    # .load() 
    
    mySchema = StructType([ StructField("reading_value", FloatType(), True)\
                       ,StructField("id", IntegerType(), True)\
                       ,StructField("readingtype", StringType(), True)\
                       ,StructField("meterpointid", IntegerType(), True)\
                       ,StructField("datetime", StringType(), True)\
                       ,StructField("createddate", StringType(), True)\
                       ,StructField("meterreadingsource", StringType(), True)\
                       ,StructField("reading_id", LongType(), True)\
                       ,StructField("register_id", IntegerType(), True)\
                       ,StructField("meter_point_id", IntegerType(), True)\
                       ,StructField("account_id", IntegerType(), True)\
                       ,StructField("reading_registerid", IntegerType(), True)\
                       ,StructField("row_no", IntegerType(), True)])
                       

    sc_reading = spark.createDataFrame(ref_readings, schema=mySchema)
    
    print(type(sc_reading))
    sc_reading.printSchema()
    sc_reading.registerTempTable("readings_rs")
    spark.sql("select count(1) from readings_rs limit 2").show()
    

    
    

    s3_df = spark.read \
    .format("com.databricks.spark.csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("s3n://igloo-uat-bucket/ensek-meterpoints/Readings/meter_point_readings_1865_2147.csv") \

    # s3_df.show()
    print(type(s3_df))
    s3_df.printSchema()
    s3_df.registerTempTable('readings_s3')
    spark.sql("select count(1) from readings_s3 limit 2").show()

    
    spark.sql("select count(1) from readings_s3 s \
                left outer join readings_rs r  \
                ON s.reading_id = r.reading_id \
                ").show()

    # file = Path('ref_readings_B.csv')
    # if not file.exists():
    #     ref_readings.to_csv('ref_readings_B.csv', index=False)

    # ref_meterpoints.to_csv('ref_meterpoints_B.csv', index=False)
    # ref_meters.to_csv('ref_meters_B.csv', index=False)
    # ref_metersattributes.to_csv('ref_metersattributes_B.csv', index=False)
    # ref_registers.to_csv('ref_registers_B.csv', index=False)
    # ref_registersattributes.to_csv('ref_registersattributes_B.csv', index=False)
    # ref_attributes.to_csv('ref_attributes_B.csv', index=False)
    # print(ref_readings.head(2))

    # ref_readings_B = pd.read_csv('ref_readings_B.csv')
    # ref_readings_join = ref_readings_B.merge(ref_readings[(ref_readings.reading_value != ref_readings_B.reading_value) | (ref_readings.readingtype != ref_readings_B.readingtype)][['reading_id']], left_on='reading_id', right_on='reading_id', how='right')
    # ref_readings_join.to_csv('readings_updated.csv', index=False)

    # reading_out = ref_readings_join[ref_readings_join.reading_value != ref_readings_join.reading_value_B]
    # print(reading_out)

def main():
   extract_meterpoints_data()



if __name__ == "__main__":
    main()