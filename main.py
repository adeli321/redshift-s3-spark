import os
import boto3
from pyspark import SparkConf
from pyspark.sql import SparkSession 
from pyspark.sql.types import *

########
# TO TEST LOCALLY
# pyspark --packages com.databricks:spark-redshift_2.10:3.0.0-preview1 --jars RedshiftJDBC42-no-awssdk-1.2.20.1043.jar,aws-java-sdk-1.11.534.jar
# pyspark --packages com.databricks:spark-redshift_2.10:3.0.0-preview1,org.apache.hadoop:hadoop-aws:3.2.0 --jars RedshiftJDBC42-no-awssdk-1.2.20.1043.jar,jar_files
# pyspark --packages com.databricks:spark-redshift_2.10:3.0.0-preview1,org.apache.hadoop:hadoop-aws:3.2.0 --jars jar_files
# pyspark --packages org.apache.hadoop:hadoop-aws:3.2.0 --jars jar_files
########
# pyspark --packages org.apache.hadoop:hadoop-aws:2.7.0 --jars hadoop-aws-2.7.0.jar
########
# spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.0 --jars hadoop-aws-2.7.0.jar --py-files dependencies.zip main.py
# spark-submit --packages org.apache.hadoop:hadoop-aws:3.1.2

aws_access_key = os.environ.get('AWS_ACCESS_KEY')
aws_secret_key = os.environ.get('AWS_SECRET_KEY')

spark = SparkSession.builder \
                    .master('local') \
                    .appName('RedshiftEtl') \
                    .getOrCreate()
                    # .config("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem") \
                    # .getOrCreate()
# SparkConf().getAll()
sc = spark.sparkContext

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_key)
# sc._jsc.hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

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

# schema_18_types = StructType([
#             StructField('date', DateType(), True),
#             StructField('time', TimestampType(), True), 
#             StructField('server_ip', StringType(), True),
#             StructField('method', StringType(), True),
#             StructField('uri_stem', StringType(), True),
#             StructField('uri_query', StringType(), True),
#             StructField('server_port', IntegerType(), True),
#             StructField('username', StringType(), True),
#             StructField('client_ip', StringType(), True),
#             StructField('client_browser', StringType(), True),
#             StructField('client_cookie', StringType(), True),
#             StructField('client_referrer', StringType(), True),
#             StructField('status', IntegerType(), True),
#             StructField('substatus', IntegerType(), True),
#             StructField('win32_status', IntegerType(), True),
#             StructField('bytes_sent', IntegerType(), True),
#             StructField('bytes_received', IntegerType(), True),
#             StructField('duration', IntegerType(), True)])

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

s3_client = boto3.client('s3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key)

# bucket_contents = s3_client.list_objects(Bucket='la-ticket-bucket-eu', Prefix='BI_logs_small/')
# file_names = []
# for i in bucket_contents['Contents']:
#     file_names.append(i['Key'])
# file_names.pop(0) # remove 'BI_logs' folder name from list, because it's not a specific file name
# for log in file_names:

# files = ['BI_logs/u_ex110209.log', 'BI_logs/u_ex091123.log']

# for i in files:

file_object = s3_client.get_object(Bucket='la-ticket-bucket-eu', 
                                        Key='BI_logs/u_ex100106.log')
file_contents = file_object['Body'].read().decode().split('\n')
# print(f'------------------------------------{log}-----------------------------------')

# logs with 18 columns
logs_18 = []
# logs with 14 columns
logs_14 = []

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

    df_14        = spark.createDataFrame(para_logs_14, schema_14)
    
    df_14 = df_14.withColumn('date', df_14['date'].cast(DateType()))
    df_14 = df_14.withColumn('time', df_14['time'].cast(TimestampType()))
    df_14 = df_14.withColumn('server_port', df_14['server_port'].cast(IntegerType()))
    df_14 = df_14.withColumn('status', df_14['status'].cast(IntegerType()))
    df_14 = df_14.withColumn('substatus', df_14['substatus'].cast(IntegerType()))
    df_14 = df_14.withColumn('win32_status', df_14['win32_status'].cast(IntegerType()))
    df_14 = df_14.withColumn('duration', df_14['duration'].cast(IntegerType()))

    df_14.write.mode('append').save('s3a://la-ticket-bucket-eu/spark-etl5')
    print('Wrote DF to spark-etl5 14 columns')

######## Attempt to write from spark directly to AWS Redshift
######## Apparently this is bad practice, so writing to s3 instead
# df_18.write \
#   .format("com.databricks.spark.redshift") \
#   .option("url", "jdbc:redshift://la-tickets.cwnesvytuyhn.eu-west-1.redshift.amazonaws.com:5439/dev?user=awsuser&password=Lovesaws22") \
#   .option("dbtable", "sparktest") \
#   .option("tempdir", "s3://la-ticket-bucket-eu/spark-etl-temp") \
#   .option("aws_iam_role", "arn:aws:iam::900056063831:role/aws-service-role/redshift.amazonaws.com/AWSServiceRoleForRedshift") \
#   .mode("append") \
#   .save()

