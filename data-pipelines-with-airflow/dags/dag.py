from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from helpers import SqlQueries

import os
import logging

def create_tables(*args, **kwargs):
    """
        This function is used to create 7 tables (2 staging, 5 dimension, 1 fact) in the Redshift cluster
    """
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    create_tables_list = SqlQueries.CREATE_TABLE_LIST
    
    for sql_statement in create_tables_list:
        redshift_hook.run(sql_statement)
        
    logging.info("7 tables SUCCESSFULLY created")
    
    
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# Guideline parameters: 
    # The DAG does not have dependencies on past runs
    # On failure, the task are retried 3 times
    # Retries happen every 5 minutes
    # Catchup is turned off
    # Do not email on retry
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds = 5),
    'email_on_retry': False,
    'catchup': False,
    'start_date': datetime.now() # TO-DO: Remove after testing
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

''' Remove task, as per the specification
create_tables_task = PythonOperator(
    task_id = 'create_tables', 
    dag = dag,
    python_callable = create_tables,
    provide_context = True
)
'''

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    dag = dag, 
    aws_credentials_id = "aws_credentials",
    redshift_conn_id = "redshift",
    table = "public.staging_events",
    source = "s3://udacity-dend/log_data", 
    json_headers = "s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag, 
    aws_credentials_id = "aws_credentials",
    redshift_conn_id = "redshift",
    table = "public.staging_songs",
    source = "s3://udacity-dend/song_data", 
    json_headers = ""
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag, 
    redshift_conn_id = "redshift",
    table = 'public.songplays',
    sql_command = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag, 
    redshift_conn_id = "redshift",
    table = 'public.users',
    sql_command = SqlQueries.user_table_insert, 
    is_truncate_mode = False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag, 
    redshift_conn_id = "redshift",
    table = 'public.songs',
    sql_command = SqlQueries.song_table_insert, 
    is_truncate_mode = False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag, 
    redshift_conn_id = "redshift",
    table = 'public.artists',
    sql_command = SqlQueries.artist_table_insert, 
    is_truncate_mode = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag, 
    redshift_conn_id = "redshift",
    table = 'public.time',
    sql_command = SqlQueries.time_table_insert, 
    is_truncate_mode = True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag, 
    redshift_conn_id = "redshift",
    table_list = ['public.time', 'public.artists', 'public.songs', 
                  'public.users', 'public.songplays'], 
    sql_command = "SELECT COUNT(*) FROM {};",
    valid_row_count = 1
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Declare task dependencies

# start_operator >> create_tables_task

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_time_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table

run_quality_checks << load_time_dimension_table
run_quality_checks << load_artist_dimension_table
run_quality_checks << load_song_dimension_table
run_quality_checks << load_user_dimension_table

run_quality_checks >> end_operator
