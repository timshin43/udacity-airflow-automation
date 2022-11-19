import datetime
import logging
# import sql_queries

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from helpers import SqlQueries 

# def list_keys():
#     hook = S3Hook(aws_conn_id='aws_credentials')
#     bucket = Variable.get('s3_bucket')
#     prefix = Variable.get('s3_prefix_log_data')
#     logging.info(f"Listing Keys from {bucket}/{prefix}")
#     keys = hook.list_keys(bucket, prefix=prefix)
#     for key in keys:
#         logging.info(f"- s3://{bucket}/{key}")
#     print (SqlQueries.songplay_table_insert)
        
# list_keys()
print(SqlQueries.songplay_table_insert)