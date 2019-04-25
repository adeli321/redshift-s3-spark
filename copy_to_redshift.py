import os
from redshift_connect import UseRedshift

aws_access_key = os.environ.get('AWS_ACCESS_KEY')
aws_secret_key = os.environ.get('AWS_SECRET_KEY')
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

def copy_to_redshift():
    """ Executes an SQL command to copy s3 folder to a Redshift table """
    try:
        with UseRedshift(redshift_db_config) as cursor: 
            SQL = rf"""COPY public.sparktest FROM 's3://la-ticket-bucket-eu/spark-etl5'
                IAM_ROLE 'arn:aws:iam::900056063831:role/RedshiftCopyUnload'
                FORMAT AS PARQUET;"""
            cursor.execute(SQL)
    except Exception as err:
        print('Error: ', err)

if __name__ == '__main__':
    copy_to_redshift()


