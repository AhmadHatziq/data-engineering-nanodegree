import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

"""
    This file represents the core logic in the ETL process.
    There are 2 main methods:
        1. Load to staging (S3 -> Redshift, schema: staging) 
        2. Process for end-users (Redshift, schema:staging -> Redshift, schema:star)
            see: https://knowledge.udacity.com/questions/444148
"""

def load_staging_tables(cur, conn):
    """
        Loads the data from S3 to STAGING
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """
        Loads the data from STAGING to STAR SCHEMA
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Read config files
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    host, dbname, user, = config.get("DWH","dwh_endpoint"), config.get("DWH", "dwh_db"),  config.get("DWH", "dwh_db_user")
    password, port =  config.get("DWH", "dwh_db_password"),  config.get("DWH", "dwh_port")

    # Connect to cloud
    connection_string = "host={} dbname={} user={} password={} port={}".format(host, dbname, user, password, port)
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()

    # Load to staging (S3 -> Staging Redshift)
    print("Starting load from S3 -> STAGING")
    load_staging_tables(cur, conn)
    print("Completed load to STAGING")
    
    # Insert to star_schema tables (Redshift -> Fact/Dim Tables)
    print("Starting load from STAGING -> STAR SCHEMA")
    insert_tables(cur, conn)
    print("Completed load to STAR SCHEMA")

    conn.close()
    
    print("ETL completed")


if __name__ == "__main__":
    main()