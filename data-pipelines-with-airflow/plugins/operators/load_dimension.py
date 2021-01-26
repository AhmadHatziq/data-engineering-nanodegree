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
                 redshift_conn_id = "",
                 table = "",
                 sql_command = "",
                 is_truncate_mode = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_command = sql_command
        self.is_truncate_mode = is_truncate_mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        final_sql_command = ("""
            INSERT INTO {}
            {} ;
        """).format(self.table, self.sql_command)
        
        # If delete mode, need to truncate table first. Else, just insert data.
        if self.is_truncate_mode == True:
            truncate_command = ("TRUNCATE TABLE {} ;").format(self.table)
            final_sql_command = truncate_command + final_sql_command 
           
        redshift.run(final_sql_command)
        
        # Logging
        if self.is_truncate_mode == True:
            self.log.info(("Truncated and Loaded data into {}").format(self.table))
        else:
            self.log.info(("Loaded data into {}").format(self.table))
            
            
