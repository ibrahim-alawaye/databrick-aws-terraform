import boto3
import os
from dotenv import load_dotenv, set_key
from pathlib import Path

env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

def create_security_group_for_vpc():
    vpc_id = os.getenv("VPC_ID")
    security_group_name = os.getenv("SECURITY_GROUP_NAME")
    security_group_description = os.getenv("SECURITY_GROUP_DESCRIPTION")
    
    if not vpc_id:
        print("VPC_ID is not set in the .env file.")
        return

    ec2_client = boto3.client(
        'ec2',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION")
    )

    try:
        vpc_response = ec2_client.describe_vpcs(VpcIds=[vpc_id])
        if not vpc_response['Vpcs']:
            print(f"No VPC found with ID: {vpc_id}")
            return
        print(f"VPC {vpc_id} found.")
    except Exception as e:
        print(f"Error finding VPC: {e}")
        return

    try:
        subnets_response = ec2_client.describe_subnets(Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}])
        subnets = subnets_response['Subnets']
        if len(subnets) < 2:
            print(f"Not enough subnets found in VPC {vpc_id}. At least 2 subnets are required.")
            return
        subnet_id1 = subnets[0]['SubnetId']
        subnet_id2 = subnets[1]['SubnetId']
        print(f"Found Subnet 1 with ID: {subnet_id1}")
        print(f"Found Subnet 2 with ID: {subnet_id2}")
    except Exception as e:
        print(f"Error retrieving subnets: {e}")
        return
    try:
        security_group_response = ec2_client.create_security_group(
            GroupName=security_group_name,
            Description=security_group_description,
            VpcId=vpc_id
        )
        security_group_id = security_group_response['GroupId']
        print(f"Created Security Group with ID: {security_group_id}")
    except Exception as e:
        print(f"Error creating security group: {e}")
        return

    try:
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
    except Exception as e:
        print(f"Error setting inbound rules: {e}")
        return

    set_key(env_path, "VPC_ID", vpc_id)
    set_key(env_path, "SECURITY_GROUP_ID", security_group_id)
    set_key(env_path, "SUBNET_ID1", subnet_id1)
    set_key(env_path, "SUBNET_ID2", subnet_id2)

    print("Security Group, VPC, and Subnet IDs saved to .env file.")

if __name__ == "__main__":
    create_security_group_for_vpc()
