import configparser
import os
import pandas as pd
import psycopg2
import sql_statements 

def perform_quality_checks(cur, conn):
    '''
        Performs the data quality checks. 
        
    Args:
        param1 (cursor object to Redshift): The first parameter.
        param2 (conn object to Redshift): The second parameter.
    '''
    
    check_row_counts(cur, conn)
    check_error_threshold_of_matching_ids(cur, conn)
    return

def check_row_counts(cur, conn):
    '''
        Checks if the row counts for each table is more than 0.
        If row count is <= 0, raise error. 
        
    Args:
        param1 (cursor object to Redshift): The first parameter.
        param2 (conn object to Redshift): The second parameter.
    '''
    
    table_names = [
    'IMDB_TITLE_AKAS', 'IMDB_TITLE_PRINCIPALS', 'IMDB_TITLE_BASICS', 'IMDB_TITLE_CREW', 'IMDB_TITLE_EPISODE', 
    'IMDB_TITLE_RATINGS', 'IMDB_NAME_BASICS', 'KAGGLE_MOVIES_METADATA', 'KAGGLE_KEYWORDS', 'KAGGLE_CREDITS', 
    'KAGGLE_LINKS', 'KAGGLE_RATINGS'
    ]
    
    for table in table_names:
        sql_string = "SELECT COUNT(*) FROM " + table + ";"
        cur.execute(sql_string)
        conn.commit()
        row = cur.fetchone()
        num_rows = row[0]

        if num_rows <= 0:
            raise ValueError('Data Quality ERROR: table ' + table + ' has 0 rows.')
            
    return
        
def check_error_threshold_of_matching_ids(cur, conn):
    '''
        The purpose of the pipeline is to consolidate the information between the IMDB and TMDB databases.
        The common attribute that they share is the IMDB TITLE ID. The table KAGGLE_LINK links the 2 databases together.
        This function checks if the number of TITLE_IDs within TMDB, that does not exist within IMDB, is within an error threshold.
        The error margin is set at 0.2% x TOTAL_RECORDS_IN_KAGGLE_LINK.
        We tolerate an error margin as the KAGGLE TMDB dataset is obtained via web-scrapping, which is prone to errorneous data entries.
        
    Args:
        param1 (cursor object to Redshift): The first parameter.
        param2 (conn object to Redshift): The second parameter.
    '''
    
    # Get number of non_existing IDs
    query = "SELECT COUNT(*) FROM KAGGLE_LINKS WHERE KAGGLE_LINKS.IMDB_ID NOT IN (SELECT IMDB_TITLE_PRINCIPALS.TITLE_ID FROM IMDB_TITLE_PRINCIPALS );"
    cur.execute(query)
    conn.commit()
    row = cur.fetchone()
    missing_imdb_ids = row[0]

    # Get total counts of KAGGLE_LINKS table
    query = "SELECT COUNT(*) FROM KAGGLE_LINKS"
    cur.execute(query)
    conn.commit()
    row = cur.fetchone()
    kaggle_imdb_counts = row[0]
    
    # Checks if error proportion is tolerable
    missing_proportion = missing_imdb_ids / kaggle_imdb_counts

    if missing_proportion >= (0.002 * kaggle_imdb_counts):
        raise ValueError("Error threshold exceeded, please check data quality of source files.")
        
    return
