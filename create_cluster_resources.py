# Import required libraries
import configparser
import boto3
import pandas as pd
import json
import time

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

# Connection to AWS services (IAM, EC2, REDSHIFT)
iam = boto3.client(
    'iam',
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
    aws_session_token=SESSION_TOKEN,
    region_name='us-east-1'
)

ec2 = boto3.resource(
    'ec2',
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

# Create IAM Redshift Role & attach policies
print("Let's delete existing IAM role: ")
try:
    iam.detach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    print("Deletion of Role is successful.")
    print()
except Exception as e:
    print("Error in deleting the role:")
    print(e)
    print()

print("Let's create a new IAM role: ")
try:
    dwhRole = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description="Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {
                'Statement': [
                    {
                        'Action': 'sts:AssumeRole',
                        'Effect': 'Allow',
                        'Principal': {'Service': 'redshift.amazonaws.com'}
                    }
                ],
                'Version': '2012-10-17'
            }
        )
    )
    print("Creation of Role is successful.")
    print()
except Exception as e:
    print("Error in creating the role:")
    print(e)
    print()

print("Let's attach role policy: ")
try:
    iam.attach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )['ResponseMetadata']['HTTPStatusCode']

    print("Policy attached successfully.")
    print()
except Exception as e:
    print("Error in attaching the policy:")
    print(e)
    print()

# Creating the Redshift Cluster
print("Let's delete any existing cluster: ")
try:
    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)
    print("Cluster deletion initiated...")
    print()

    # Waiting mechanism for cluster deletion
    while True:
        try:
            response = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
            cluster_info = response['Clusters'][0]  # Get the cluster info
            cluster_status = cluster_info['ClusterStatus']

            print(f"Current Cluster Status: {cluster_status}")

            if cluster_status == 'deleted':
                print("Cluster has been deleted successfully!")
                break  # Exit the loop if the cluster is deleted
            elif cluster_status in ['deleting', 'failed']:
                print("Retrying in 15 seconds.")

            time.sleep(15)  # Wait for 15 seconds before checking again
        except:
            print("Cluster has been deleted successfully!")
            print()
            break

except Exception as e:
    print("Error in deleting the cluster:")
    print(e)
    print()

print("Let's create the cluster: ")
try:
    response = redshift.create_cluster(
        # Identifiers & Credentials
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,

        # Roles (for S3 access)
        IamRoles=[DWH_ROLE_ARN]
    )

    print("Cluster created successfully!")
    print()
except Exception as e:
    print("Error in creating cluster:")
    print(e)
    print()

# Check the cluster status
print("Checking the cluster status: ")
try:
    response = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
    cluster_info = response['Clusters'][0]  # Assuming there's only one cluster with the given identifier
    print()

    # Waiting mechanism
    while True:
        # Describe the cluster to get the latest status
        response = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
        cluster_info = response['Clusters'][0]
        cluster_status = cluster_info['ClusterStatus']

        print(f"Current Cluster Status: {cluster_status}")

        if cluster_status == 'available':
            print("Cluster is available!")
            break  # Exit the loop if the cluster is available
        elif cluster_status in ['deleting', 'failed']:
            print("Retrying in 15 seconds.")

        time.sleep(15)  # Wait for 15 seconds before checking again
    print()
except Exception as e:
    print("Error describing cluster:")
    print(e)
    print()

# Security group parameters configuration
try:
    vpc = ec2.Vpc(id=DWH_VPC_ID)
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)

    # Define the rule parameters
    cidr_ip = '0.0.0.0/0'
    ip_protocol = 'tcp'
    from_port = int(DWH_PORT)
    to_port = int(DWH_PORT)

    # Check existing ingress rules
    existing_rules = defaultSg.ip_permissions
    rule_exists = False

    for rule in existing_rules:
        # Check if the rule matches the desired parameters
        if (rule['IpProtocol'] == ip_protocol and
                rule['FromPort'] == from_port and
                rule['ToPort'] == to_port and
                any(cidr['CidrIp'] == cidr_ip for cidr in rule['IpRanges'])):
            rule_exists = True
            break

    if rule_exists:
        print(f"Rule already exists: ALLOW {ip_protocol} from {cidr_ip} on port {from_port}.")
        print()
    else:
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp=cidr_ip,
            IpProtocol=ip_protocol,
            FromPort=from_port,
            ToPort=to_port
        )
        print("Security group configuration is successful.")
        print()

except Exception as e:
    print("Error in configuring security group:")
    print(e)
    print()

# Update endpoint in the config file
response = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
DWH_ENDPOINT = config.set("DWH", "DWH_ENDPOINT", response['Endpoint']['Address'])

# Write the endpoint & VPC ID back to the config file
with open('dwh.cfg', 'w') as configfile:
    config.write(configfile)

print("End")
