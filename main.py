#!/usr/bin/env python3

import os
import boto3
from pyspark import SparkConf
from pyspark.sql import SparkSession 
from pyspark.sql.types import *

########
# TO TEST LOCALLY
# spark-submit --jars jar_files/hadoop-common-2.7.3.jar,jar_files/hadoop-aws-2.7.3.jar,jar_files/aws-java-sdk-1.7.4.jar main.py
########

aws_access_key = os.environ.get('AWS_ACCESS_KEY')
aws_secret_key = os.environ.get('AWS_SECRET_KEY')

spark = SparkSession.builder \
                    .master('local') \
                    .appName('RedshiftEtl') \
                    .getOrCreate()
sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_key)

# Schema for the 18 columns of logs to be loaded as strings initially
schema_18 = StructType([
            StructField('date', StringType(), True),
            StructField('time', StringType(), True), 
            StructField('server_ip', StringType(), True),
            StructField('method', StringType(), True),
            StructField('uri_stem', StringType(), True),
            StructField('uri_query', StringType(), True),
            StructField('server_port', StringType(), True),
            StructField('username', StringType(), True),
            StructField('client_ip', StringType(), True),
            StructField('client_browser', StringType(), True),
            StructField('client_cookie', StringType(), True),
            StructField('client_referrer', StringType(), True),
            StructField('status', StringType(), True),
            StructField('substatus', StringType(), True),
            StructField('win32_status', StringType(), True),
            StructField('bytes_sent', StringType(), True),
            StructField('bytes_received', StringType(), True),
            StructField('duration', StringType(), True)])

# Schema for logs with 14 columns
schema_14 = StructType([
            StructField('date', StringType(), True),
            StructField('time', StringType(), True), 
            StructField('server_ip', StringType(), True),
            StructField('method', StringType(), True),
            StructField('uri_stem', StringType(), True),
            StructField('uri_query', StringType(), True),
            StructField('server_port', StringType(), True),
            StructField('username', StringType(), True),
            StructField('client_ip', StringType(), True),
            StructField('client_browser', StringType(), True),
            StructField('status', StringType(), True),
            StructField('substatus', StringType(), True),
            StructField('win32_status', StringType(), True),
            StructField('duration', StringType(), True)])

def grab_s3_contents():
    """ Connects to s3, grabs an object from bucket, 
        and returns the contents of object.
    """
    s3_client = boto3.client('s3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key)
    file_object = s3_client.get_object(Bucket='la-ticket-bucket-eu', 
                                            Key='BI_logs/u_ex100106.log')
    file_contents = file_object['Body'].read().decode().split('\n')

    return file_contents


def transform_logs(file_contents):
    """ Takes a file's contents, transforms its data types,  
        converts it to Parquet files, and writes to s3. 
    """
    # Logs with 18 columns
    logs_18 = []
    # Logs with 14 columns
    logs_14 = []

    # Ignore comments and split each log column by space
    for i in file_contents:
        if i.startswith('#'):
            pass
        else:
            if len(i.split(' ')) == 18:
                logs_18.append(i.split(' '))
            else:
                logs_14.append(i.split(' '))

    if logs_18 == [['']]:
        pass
    else:
        # Parallelize the logs into an RDD with n partitions
        para_logs_18 = sc.parallelize(logs_18, 10)
        # Create a dataframe from RDD using the schema previously made
        df_18 = spark.createDataFrame(para_logs_18, schema_18)

        # Convert log columns to appropriate datatypes
        df_18 = df_18.withColumn('date', df_18['date'].cast(DateType()))
        df_18 = df_18.withColumn('time', df_18['time'].cast(TimestampType()))
        df_18 = df_18.withColumn('server_port', df_18['server_port'].cast(IntegerType()))
        df_18 = df_18.withColumn('status', df_18['status'].cast(IntegerType()))
        df_18 = df_18.withColumn('substatus', df_18['substatus'].cast(IntegerType()))
        df_18 = df_18.withColumn('win32_status', df_18['win32_status'].cast(IntegerType()))
        df_18 = df_18.withColumn('bytes_sent', df_18['bytes_sent'].cast(IntegerType()))
        df_18 = df_18.withColumn('bytes_received', df_18['bytes_received'].cast(IntegerType()))
        df_18 = df_18.withColumn('duration', df_18['duration'].cast(IntegerType()))

        # Writes dataframe as n number of Parquet files to S3
        df_18.write.mode('append').save('s3a://la-ticket-bucket-eu/spark-etl5')
        print('Wrote DF to spark-etl5 18 columns')

    if logs_14 == [['']]:
        pass
    else:
        para_logs_14 = sc.parallelize(logs_14, 10)
        df_14 = spark.createDataFrame(para_logs_14, schema_14)
        
        df_14 = df_14.withColumn('date', df_14['date'].cast(DateType()))
        df_14 = df_14.withColumn('time', df_14['time'].cast(TimestampType()))
        df_14 = df_14.withColumn('server_port', df_14['server_port'].cast(IntegerType()))
        df_14 = df_14.withColumn('status', df_14['status'].cast(IntegerType()))
        df_14 = df_14.withColumn('substatus', df_14['substatus'].cast(IntegerType()))
        df_14 = df_14.withColumn('win32_status', df_14['win32_status'].cast(IntegerType()))
        df_14 = df_14.withColumn('duration', df_14['duration'].cast(IntegerType()))

        df_14.write.mode('append').save('s3a://la-ticket-bucket-eu/spark-etl5')
        print('Wrote DF to spark-etl5 14 columns')

if __name__ == '__main__':
    file_contents = grab_s3_contents()
    transform_logs(file_contents)