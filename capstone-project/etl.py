import configparser
import os
import pandas as pd
import psycopg2
import sql_statements 
import time
from data_quality import perform_quality_checks
from source_file_to_parquet import clean_data
from parquet_to_redshift import parquet_to_redshift
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat, col, lit, length
from pyspark.sql.types import IntegerType, DoubleType, LongType

"""
    Load AWS Keys from config file. Register vars as environment variables.
"""
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
os.environ["AWS_ACCESS_KEY_ID"] = config.get('AWS','key')
os.environ["AWS_SECRET_ACCESS_KEY"] = config.get('AWS','secret')

def get_spark():
    """
        Creates spark instance.
        
    Returns:
        Spark context session object.
        
    """
    spark_session = SparkSession.builder\
                     .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0")\
                     .getOrCreate()
    return spark_session

def main():
    """
        Main driver for ETL process.
    """
    # Record start time for logging purposes
    start_time = time.time()
    
    # Get Spark object
    spark = get_spark()
    
    # Read config files to obtain redshift credentials
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    host, dbname, user, = config.get("DWH","dwh_endpoint"), config.get("DWH", "dwh_db"),  config.get("DWH", "dwh_db_user")
    password, port =  config.get("DWH", "dwh_db_password"),  config.get("DWH", "dwh_port")

    # Connect to redshift
    connection_string = "host={} dbname={} user={} password={} port={}".format(host, dbname, user, password, port)
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()
    
    # Clean source files ie convert .csv and .tsv files to .parquet using Spark
    print("Converting source files to .parquet")
    clean_data(spark)
    
    # Pull the parquet files from S3 to Redshift
    print("Exporting parquet files from S3 to Redshift")
    parquet_to_redshift(cur, conn)
    
    # Perform data quality checks
    perform_quality_checks(cur, conn)
    
    # Disconnect from redshift
    conn.close()
    
    # Record end time for logging purposes
    end_time = time.time()
    total_time = end_time - start_time
    
    print("ETL process completed.")
    print('Total time taken (in seconds): ', total_time)


if __name__ == "__main__":
    main()
