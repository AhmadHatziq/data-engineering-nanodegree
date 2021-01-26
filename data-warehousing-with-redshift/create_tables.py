import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, create_schema_queries, drop_schema_queries

"""
    This file contains methods to CREATE and DROP the
    FACT and DIM tables for the star schema in Redshift.
"""

def drop_tables(cur, conn):
    '''Dropping the schemas will automatically removed tables within the schema due to CASCADE'''
    for query in drop_schema_queries: 
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    '''Creates the schemas and then the tables'''
    for query in create_schema_queries:
        cur.execute(query)
        conn.commit()
    
    for query in create_table_queries:
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
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    
    print("Table creation SUCCESS")

if __name__ == "__main__":
    main()