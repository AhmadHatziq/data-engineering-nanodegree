import boto3
import configparser
import matplotlib.pyplot as plt
import pandas as pd
import json
import time
import sys

"""
    This file creates the red shift cluster given the user settings located
    in dwh.cfg
"""

def main():
    # Read in config data
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")
    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
    debug_df = pd.DataFrame({"Param":
                                      ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", 
                                       "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
                             "Value":
                                      [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, 
                                       DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
                             })
    
    # Create EC2, S3, IAM and RedShift clients. Clients are pre-configured to use region: us-west-2
    ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
    s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )
    iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )
    redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
    
    # Create IAM role
    dwhRole = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
        )   
    
    # Attach IAM policy
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']
    
    # Get Arn Role
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    
    # Create Red Shift cluster
    print("Cluster creation process started.")
    response = redshift.create_cluster(        
        # hardware params
        ClusterType = DWH_CLUSTER_TYPE,
        NodeType = DWH_NODE_TYPE,
        NumberOfNodes = int(DWH_NUM_NODES),

        # identifiers & credentials params
        DBName = DWH_DB,
        ClusterIdentifier = DWH_CLUSTER_IDENTIFIER,
        MasterUsername = DWH_DB_USER,
        MasterUserPassword = DWH_DB_PASSWORD,
        
        # add parameter for role (to allow s3 access)
        IamRoles=[roleArn]  
    )
    
    # Define function to extract key info from RedShift
    def prettyRedshiftProps(props):
        pd.set_option('display.max_colwidth', -1)
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        x = [(k, v) for k,v in props.items() if k in keysToShow]
        return pd.DataFrame(data=x, columns=["Key", "Value"])
    
    # Wait until cluster has been created
    print('')
    output_str = "Waiting for cluster creation."
    sys.stdout.write(output_str)  
    sys.stdout.flush()
    while True:
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        df = prettyRedshiftProps(myClusterProps)
        row = df.loc[df['Key'] == 'ClusterStatus']
        clusterStatus = row['Value'].values[0]
        if clusterStatus == 'available':
            break
        time.sleep(10) 
        output_str = "."
        sys.stdout.write(output_str)  
        sys.stdout.flush()
    print("RedShift cluster created SUCCESSFULLY\n")
    
    # Extract DWH_ENDPOINT and DWH_ROLE_ARN
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
    
    # Save DWH_ENDPOINT and DWH_ROLE_ARN to config file
    config['DWH']['DWH_ENDPOINT'] = DWH_ENDPOINT
    config['DWH']['DWH_ROLE_ARN'] = DWH_ROLE_ARN
    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile)
        
    print("\nDWH_ENDPOINT & DWH_ROLE_ARN successfully saved to CONFIG")
    


if __name__ == "__main__":
    main()
