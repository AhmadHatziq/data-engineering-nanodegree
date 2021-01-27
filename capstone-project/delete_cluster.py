import boto3
import configparser

"""
    This file delete the red shift cluster given the user settings located
    in dwh.cfg
"""

def main():
    # Read config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
    
    # Create RedShift and IAM clients
    redshift_client = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
    iam_client = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )
    
    # Delete cluster
    redshift_client.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    
    # Delete IAM role
    iam_client.detach_role_policy(RoleName = DWH_IAM_ROLE_NAME, PolicyArn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam_client.delete_role(RoleName = DWH_IAM_ROLE_NAME) 
    
    # Set DWH_ENDPOINT and DWH_ROLE_ARN to null in the CONFIG file
    config['DWH']['DWH_ENDPOINT'] = 'null'
    config['DWH']['DWH_ROLE_ARN'] = 'null'
    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile)
    
    print("Cluster is being deleted. Please check the GUI for more details.")

if __name__ == "__main__":
    main()
    