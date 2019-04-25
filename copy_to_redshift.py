import os
from redshift_connect import UseRedshift

# This script creates a table in Redshift if it doesn't
# exist, then it takes Parquet files from an s3 bucket and 
# copies them to the table (sparktest) in Redshift

db_name        = os.environ.get('DB_NAME')
redshift_user  = os.environ.get('REDSHIFT_USER')
redshift_pw    = os.environ.get('REDSHIFT_PW')
redshift_endpt = os.environ.get('REDSHIFT_ENDPT')
redshift_port  = os.environ.get('REDSHIFT_PORT')

redshift_db_config = {'dbname': db_name,
                    'host': redshift_endpt,
                    'port': redshift_port,
                    'user': redshift_user,
                    'password': redshift_pw}

def create_table():
    """Creates a table in AWS Redshift if it doesn't already exist."""
    try:
        with UseRedshift(redshift_db_config) as cursor: 
            SQL_CREATE = rf"""CREATE TABLE IF NOT EXISTS public.sparktest (
                    date DATE, 
                    time TIMESTAMP, 
                    server_ip VARCHAR(20), 
                    method VARCHAR(10), 
                    uri_stem VARCHAR(80), 
                    uri_query VARCHAR(1000), 
                    server_port INT, 
                    username VARCHAR(60), 
                    client_ip VARCHAR(20), 
                    client_browser VARCHAR(1000), 
                    client_cookie VARCHAR(1000), 
                    client_referrer VARCHAR(1000), 
                    status INT, 
                    substatus INT, 
                    win32_status INT, 
                    bytes_sent INT, 
                    bytes_received INT, 
                    duration INT
                    )"""
            cursor.execute(SQL_CREATE)
    except Exception as err:
        print('Error: ', err)

def copy_to_redshift():
    """Executes an SQL command to copy s3 folder to a Redshift table."""
    try:
        with UseRedshift(redshift_db_config) as cursor: 
            SQL_COPY   = rf"""COPY public.sparktest FROM 's3://la-ticket-bucket-eu/spark-etl5'
                    IAM_ROLE 'arn:aws:iam::900056063831:role/RedshiftCopyUnload'
                    FORMAT AS PARQUET;
                    """
            cursor.execute(SQL_COPY)
    except Exception as err:
        print('Error: ', err)

if __name__ == '__main__':
    create_table()
    copy_to_redshift()
