from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 aws_credentials_id = '',
                 redshift_conn_id = '',
                 table = '',
                 source = '',
                 json_headers = '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.source = source
        self.json_headers = json_headers
     
    def execute(self, context):
        
        # Get credentials and connection
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if "song" in self.source:
            # Loading song json data
            sql_command = ("""
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}' 
                SECRET_ACCESS_KEY '{}' 
                FORMAT AS JSON 'auto'
                region 'us-west-2';
            """).format(self.table, self.source, credentials.access_key, credentials.secret_key)
            
            redshift.run(sql_command)
            self.log.info('Song json data moved from S3 to {}'.format(self.table))
            
            
        elif "log" in self.source:
            # Loading event log json data
            sql_command = ("""
                COPY {} 
                FROM '{}' 
                ACCESS_KEY_ID '{}' 
                SECRET_ACCESS_KEY '{}' 
                FORMAT AS JSON '{}' 
                region 'us-west-2';
            """).format(self.table, self.source, credentials.access_key, credentials.secret_key, self.json_headers)
            
            redshift.run(sql_command)
            self.log.info('Event log json data moved from S3 to {}'.format(self.table))
