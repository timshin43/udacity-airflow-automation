from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 sql_stmnt="",
                 insert_flag="",
                 table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id   
        self.sql_stmnt = sql_stmnt
        self.insert_flag = insert_flag
        self.table = table

    def execute(self, context):
        self.log.info('Start LoadDimensionOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if(self.insert_flag == 'append'):
            redshift.run(self.sql_stmnt.format("",self.table))
        elif(self.insert_flag == 'truncate_insert'):
            #this runs the insert query sql_stmnt but with the extra truncate statemnt in the front
            redshift.run(self.sql_stmnt.format(f"Truncate table {self.table}; ",self.table))
        else:
            raise ValueError(f"Insert param should be append or truncate_insert")
        self.log.info('End LoadDimensionOperator')