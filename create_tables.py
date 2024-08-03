# Libraries import
import configparser
import redshift_connector
import importlib
import sql_queries

# For reloading the updated module
importlib.reload(sql_queries)

# Access updated queries
create_table_queries = sql_queries.create_table_queries
drop_table_queries = sql_queries.drop_table_queries


# Define drop table function
def drop_tables(cur, conn):
    for query in drop_table_queries:
        try:
            print(query)
            tbl_name = query.split()[4]
            cur.execute(query)
            conn.commit()
            print(f"Table '{tbl_name}' dropped successfully")
            print()
        except Exception as e:
            tbl_name = query.split()[4]
            print(f"Table '{tbl_name}' Not Present ", e)
            print()


# Define create table function
def create_tables(cur, conn):
    for query in create_table_queries:
        try:
            print(query)
            tbl_name = query.split()[2]
            cur.execute(query)
            conn.commit()
            print(f"Table '{tbl_name}' created successfully")
            print()
        except Exception as e:
            tbl_name = query.split()[2]
            print(f"Error in creating the '{tbl_name}' ", e)
            print()


# Defining main function
def main():
    # Import required data warehouse credentials using configparser
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    SESSION_TOKEN = config.get('AWS', 'SESSION_TOKEN')

    DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
    DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    DWH_DB = config.get("DWH", "DWH_DB")
    DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
    DWH_PORT = config.get("DWH", "DWH_PORT")
    DWH_ENDPOINT = config.get("DWH", "DWH_ENDPOINT")
    DWH_ROLE_ARN = config.get("DWH", "DWH_ROLE_ARN")
    DWH_VPC_ID = config.get("DWH", "DWH_VPC_ID")

    LOG_DATA = config.get("S3", "LOG_DATA")
    LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
    SONG_DATA = config.get("S3", "SONG_DATA")

    # Connect to the Redshift cluster
    conn = redshift_connector.connect(
        host=DWH_ENDPOINT,
        database=DWH_DB,
        user=DWH_DB_USER,
        password=DWH_DB_PASSWORD,
        port=DWH_PORT
    )

    # Create a cursor object
    cur = conn.cursor()

    # Calling drop tables function
    drop_tables(cur, conn)

    # Calling create tables function
    create_tables(cur, conn)

    conn.close()
    print()
    print("ALL NECESSARY TABLES HAVE BEEN CREATED SUCCESSFULLY!")


# Calling main function
if __name__ == "__main__":
    main()
