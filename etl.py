# Libraries import
import configparser
import redshift_connector
import importlib
import sql_queries

# For Reloading the updated module
importlib.reload(sql_queries)

# Access updated queries
copy_table_queries = sql_queries.copy_table_queries
insert_table_queries = sql_queries.insert_table_queries

# define load table function for staging
def load_staging_tables(cur,conn):
    for query in copy_table_queries:
        try:
            # Lets truncate and load staging table
            print(query)
            tbl_name = query.split()[1]
            print(f"COPYING DATA FROM S3 TO THE REDSHIFT TABLE '{tbl_name}':- ") 
            cur.execute('TRUNCATE TABLE {};'.format(tbl_name))
            trunc = 'SELECT COUNT(*) FROM {};'.format(tbl_name)
            cur.execute(trunc)
            row_count = cur.fetchone()[0]
            print(f"Row count before copying data into the {tbl_name}: {row_count}")

            # Execution of copy statement
            cur.execute(query)
            conn.commit()
            check = 'SELECT COUNT(*) FROM {};'.format(tbl_name)
            cur.execute(check)
            row_count = cur.fetchone()[0]  # Fetch the first result
            print(f"Row count after copying data into {tbl_name}: {row_count}")
            print()
        except Exception as e:
            tbl_name = query.split()[1]
            print(f"Error in copying the data into the '{tbl_name}' ",e)
            print()

# define insert table function
def insert_tables(cur, conn):
    for query in insert_table_queries:
        try:
            # Lets truncate and insert data into analytical tables
            print(query)
            tbl_name = query.split()[2]
            print(f"INSERTING DATA FROM STAGING TABLES TO THE DIMENSIONAL TABLES '{tbl_name}':- ") 
            cur.execute('TRUNCATE TABLE {};'.format(tbl_name))
            trunc = 'SELECT COUNT(*) FROM {};'.format(tbl_name)
            cur.execute(trunc)
            row_count = cur.fetchone()[0]
            print(f"Row count before inserting data into the {tbl_name}: {row_count}")

            # Execution of copy statement
            cur.execute(query)
            conn.commit()
            check = 'SELECT COUNT(*) FROM {};'.format(tbl_name)
            cur.execute(check)
            row_count = cur.fetchone()[0]  # Fetch the first result
            print(f"Row count after inserting data into {tbl_name}: {row_count}")
            print()
        except Exception as e:
            tbl_name = query.split()[2]
            print(f"Error in inserting the data into the '{tbl_name}' ",e)
            print()

                        
# defining main function
def main():
    # Import required datawarehouse credentials using configparser
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    KEY = config.get('AWS','KEY')
    SECRET = config.get('AWS','SECRET')
    SESSION_TOKEN = config.get('AWS','SESSION_TOKEN')
    
    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")
    DWH_ENDPOINT           = config.get("DWH","DWH_ENDPOINT")
    DWH_ROLE_ARN           = config.get("DWH","DWH_ROLE_ARN")
    DWH_VPC_ID             = config.get("DWH","DWH_VPC_ID")
    
    LOG_DATA               = config.get("S3","LOG_DATA")
    LOG_JSONPATH               = config.get("S3","LOG_JSONPATH")
    SONG_DATA               = config.get("S3","SONG_DATA")


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

    # calling load staging tables function
    load_staging_tables(cur, conn)
    
    # calling insert tables function
    insert_tables(cur, conn)

    conn.close()
    print()
    print("ETL PROCESS COMPLETED SUCCESSFULLY !")
# calling main function
if __name__ == "__main__":
    main()