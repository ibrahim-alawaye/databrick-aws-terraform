import boto3
import os
from dotenv import load_dotenv, set_key
from pathlib import Path

# Load environment variable
env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

def create_vpc_for_databricks():
    
    ec2_client = boto3.client(
        'ec2',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION")
    )

    # VPC
    vpc_response = ec2_client.create_vpc(CidrBlock='10.210.0.0/16')
    vpc_id = vpc_response['Vpc']['VpcId']
    print(f"Created VPC with ID: {vpc_id}")

    # Enable DNS support
    ec2_client.modify_vpc_attribute(
        VpcId=vpc_id,
        EnableDnsSupport={
            'Value': True
        }
    )

    #availability zones
    azs = ec2_client.describe_availability_zones()['AvailabilityZones']
    
    if len(azs) < 2:
        print("Not enough Availability Zones available.")
        return

    # Create subnets
    subnet1_response = ec2_client.create_subnet(VpcId=vpc_id, CidrBlock='10.210.1.0/24', AvailabilityZone=azs[0]['ZoneName'])
    subnet2_response = ec2_client.create_subnet(VpcId=vpc_id, CidrBlock='10.210.2.0/24', AvailabilityZone=azs[1]['ZoneName'])
    subnet_id1 = subnet1_response['Subnet']['SubnetId']
    subnet_id2 = subnet2_response['Subnet']['SubnetId']
    print(f"Created Subnet 1 with ID: {subnet_id1} in AZ: {azs[0]['ZoneName']}")
    print(f"Created Subnet 2 with ID: {subnet_id2} in AZ: {azs[1]['ZoneName']}")

    # security group
    security_group_response = ec2_client.create_security_group(
        GroupName=os.getenv("SECURITY_GROUP_NAME"),
        Description=os.getenv("SECURITY_GROUP_DESCRIPTION"),
        VpcId=vpc_id
    )
    security_group_id = security_group_response['GroupId']
    print(f"Created Security Group with ID: {security_group_id}")

    # Set inbound rules
    ec2_client.authorize_security_group_ingress(
        GroupId=security_group_id,
        IpPermissions=[
            {
                "IpProtocol": "tcp",
                "FromPort": 1025,
                "ToPort": 65535,
                "UserIdGroupPairs": [
                    {"GroupId": security_group_id}
                ]
            },
            {
                "IpProtocol": "udp",
                "FromPort": 1025,
                "ToPort": 65535,
                "UserIdGroupPairs": [
                    {"GroupId": security_group_id}
                ]
            },
            
            {
                "IpProtocol": "-1",  # -1 means all traffic
                "FromPort": 0,
                "ToPort": 65535,
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}]
            }
        ]
    )
    print("Inbound rules set for the security group.")

    # Save VPC_ID, SECURITY_GROUP_ID, and SUBNET_IDS to .env
    set_key(env_path, "VPC_ID", vpc_id)
    set_key(env_path, "SECURITY_GROUP_ID", security_group_id)
    set_key(env_path, "SUBNET_ID1", subnet_id1)
    set_key(env_path, "SUBNET_ID2", subnet_id2)

    print("VPC, Security Group, and Subnet IDs saved to .env file.")

if __name__ == "__main__":
    create_vpc_for_databricks()
