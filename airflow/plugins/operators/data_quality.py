import datetime
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
#                  table=[],
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
#         self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('Start DataQualityOperator')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for i, dq_check in enumerate(self.dq_checks):
            records = redshift_hook.get_records(dq_check['test_sql'])
            if len(records) < dq_check['expected_result'] or len(records[0]) < dq_check['expected_result']:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < dq_check['expected_result']:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
                logging.info(f"Data quality check passed with {records[0][0]} records") 
        self.log.info('End DataQualityOperator')
            
       
           