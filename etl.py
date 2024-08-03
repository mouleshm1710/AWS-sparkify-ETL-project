# Libraries import
import configparser
import redshift_connector
import importlib
import sql_queries

# For reloading the updated module
importlib.reload(sql_queries)

# Access updated queries
copy_table_queries = sql_queries.copy_table_queries
insert_table_queries = sql_queries.insert_table_queries


def load_staging_tables(cur, conn):
    """Load data into staging tables from S3.

    This function truncates each staging table and copies data from S3
    into the corresponding Redshift table. It prints the row counts
    before and after the copy operation.

    Args:
        cur: The database cursor to execute queries.
        conn: The database connection to commit changes.
    """
    for query in copy_table_queries:
        try:
            # Let's truncate and load staging table
            print(query)
            tbl_name = query.split()[1]
            print(f"COPYING DATA FROM S3 TO THE REDSHIFT TABLE '{tbl_name}':- ")
            cur.execute(f'TRUNCATE TABLE {tbl_name};')
            trunc = f'SELECT COUNT(*) FROM {tbl_name};'
            cur.execute(trunc)
            row_count = cur.fetchone()[0]
            print(f"Row count before copying data into the {tbl_name}: {row_count}")

            # Execution of copy statement
            cur.execute(query)
            conn.commit()
            check = f'SELECT COUNT(*) FROM {tbl_name};'
            cur.execute(check)
            row_count = cur.fetchone()[0]  # Fetch the first result
            print(f"Row count after copying data into {tbl_name}: {row_count}")
            print()
        except Exception as e:
            tbl_name = query.split()[1]
            print(f"Error in copying the data into the '{tbl_name}' ", e)
            print()


def insert_tables(cur, conn):
    """Insert data from staging tables into analytical tables.

    This function truncates each analytical table and inserts data from
    the corresponding staging table. It prints the row counts before
    and after the insert operation.

    Args:
        cur: The database cursor to execute queries.
        conn: The database connection to commit changes.
    """
    for query in insert_table_queries:
        try:
            # Let's truncate and insert data into analytical tables
            print(query)
            tbl_name = query.split()[2]
            print(f"INSERTING DATA FROM STAGING TABLES TO THE DIMENSIONAL TABLES '{tbl_name}':- ")
            cur.execute(f'TRUNCATE TABLE {tbl_name};')
            trunc = f'SELECT COUNT(*) FROM {tbl_name};'
            cur.execute(trunc)
            row_count = cur.fetchone()[0]
            print(f"Row count before inserting data into the {tbl_name}: {row_count}")

            # Execution of insert statement
            cur.execute(query)
            conn.commit()
            check = f'SELECT COUNT(*) FROM {tbl_name};'
            cur.execute(check)
            row_count = cur.fetchone()[0]  # Fetch the first result
            print(f"Row count after inserting data into {tbl_name}: {row_count}")
            print()
        except Exception as e:
            tbl_name = query.split()[2]
            print(f"Error in inserting the data into the '{tbl_name}' ", e)
            print()


def main():
    """Main function to execute the ETL process.

    This function imports data warehouse credentials, connects to the
    Redshift cluster, and executes the ETL process by loading
    staging tables and inserting data into analytical tables.
    """
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

    # Calling load staging tables function
    load_staging_tables(cur, conn)

    # Calling insert tables function
    insert_tables(cur, conn)

    conn.close()
    print()
    print("ETL PROCESS COMPLETED SUCCESSFULLY!")


# Calling main function
if __name__ == "__main__":
    main()
