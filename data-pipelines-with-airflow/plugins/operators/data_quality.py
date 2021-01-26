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
                 redshift_conn_id = "",
                 table_list = [],
                 sql_command = "",
                 valid_row_count = -1
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table_list = table_list
        self.redshift_conn_id = redshift_conn_id
        self.sql_command = sql_command
        self.valid_row_count = valid_row_count

    def execute(self, context):
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        # Iterate through each table
        for table in self.table_list:
            final_sql_command = (self.sql_command).format(table)
            
            # Check if command returns a valid response
            results = redshift_hook.get_records(final_sql_command)
            if len(results[0]) < 1 or len(results) < 1:
                raise ValueError(("Table {} has failed count operation").format(table))
            
            # Check if number of rows is valid
            num_rows = results[0][0]
            if num_rows < self.valid_row_count:
                raise ValueError(("Table {} has failed check as got invalid number of rows. Lesser than {}.").format(table, self.valid_row_count))
                
            # Report quality check pass for table
            self.log.info(("Table {} has SUCCESSFULLY passed data quality checks.").format(table))
                
                
                
        