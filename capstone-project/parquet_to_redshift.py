import configparser
import os
import pandas as pd
import psycopg2
import sql_statements 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat, col, lit, length
from pyspark.sql.types import IntegerType, DoubleType, LongType

def parquet_to_redshift(cur, conn):
    '''
    
        Loads the .parquet files from S3 to Redshift.
        
    Args:
        param1 (cursor object to Redshift): The first parameter.
        param2 (conn object to Redshift): The second parameter.
        
    '''
    
    # Create the tables if they do not exist yet
    create_tables(cur, conn)
    
    export_imdb_data(cur, conn)
    export_kaggle_data(cur, conn)
    
    return

def create_tables(cur, conn):
    """
        Creates the table schema if they do not exist yet.
    Args:
        param1 (cursor object to Redshift): The first parameter.
        param2 (conn object to Redshift): The second parameter.
    """
    create_table_statements = sql_statements.CREATE_TABLE_STATEMENTS
    
    for statement in create_table_statements:
        cur.execute(statement)
        conn.commit()

def export_imdb_data(cur, conn):
    """
        Processes the 7 IMDB .parquet files. 
        Performs TRUNCATE and LOAD. 
        
    Args:
        param1 (cursor object to Redshift): The first parameter.
        param2 (conn object to Redshift): The second parameter.
        
    """
    imdb_export_statements = sql_statements.COPY_IMDB_TABLES
    
    for statement in imdb_export_statements:
        cur.execute(statement)
        conn.commit()
    

def export_kaggle_data(cur, conn):
    """
        Processes the 5 Kaggle .parquet files.
        Performs TRUNCATE and LOAD. 
        
    Args:
        param1 (cursor object to Redshift): The first parameter.
        param2 (conn object to Redshift): The second parameter.
    """
    kaggle_export_statements = sql_statements.COPY_KAGGLE_TABLES
    
    for statement in kaggle_export_statements:
        cur.execute(statement)
        conn.commit()
