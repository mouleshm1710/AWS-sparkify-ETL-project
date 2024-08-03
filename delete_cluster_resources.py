# Import required libraries
import configparser
import boto3
import pandas as pd
import json

# Import Datawarehouse Credentials
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

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

# Connection to AWS services (IAM & REDSHIFT)
iam = boto3.client(
    'iam',
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
    aws_session_token=SESSION_TOKEN,
    region_name='us-east-1'
)

redshift = boto3.client(
    'redshift',
    region_name="us-east-1",
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
    aws_session_token=SESSION_TOKEN
)

print("Connection to the AWS services is successful.")
print()

# Delete resources & roles
print("Let's delete existing IAM role: ")
try:
    iam.detach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    print("Deletion of role is successful.")
    print()
except Exception as e:
    print("Error in deleting the role:")
    print(e)
    print()

print("Let's delete any existing cluster: ")
try:
    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)
    print("Cluster deletion initiated.")
    print()
except Exception as e:
    print("Error in deleting the cluster:")
    print(e)
    print()
