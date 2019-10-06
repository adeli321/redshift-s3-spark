# redshift-s3-spark
This repo is meant as an exercise in learning the basics of Spark. I re-wrote the scripts from the redshift-web-logs repository to see if using Spark would speed up the execution time. 

The main.py script takes a log file that resides in an AWS s3 bucket, extracts the contents, transforms its data types, transforms it into Parquet files (10), and writes them back to s3.
The copy_to_redshift.py script copies the Parquet files from the s3 bucket into a table in AWS Redshift.

The order of running these scripts is:
  1. main.py
  2. copy_to_redshift.py
  
To run main.py locally with spark:
  `spark-submit --jars jar_files/hadoop-common-2.7.3.jar,jar_files/hadoop-aws-2.7.3.jar,jar_files/aws-java-sdk-1.7.4.jar main.py`
