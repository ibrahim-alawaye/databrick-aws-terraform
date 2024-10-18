import boto3
import os
import json 
from dotenv import load_dotenv
from pathlib import Path


env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

def create_s3_bucket():
    bucket_name = os.getenv("S3_BUCKET_NAME")
    region = os.getenv("AWS_REGION")

    s3_client = boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )

    try:
        # Create the S3 bucket
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region}
            )

        
        bucket_arn = f"arn:aws:s3:::{bucket_name}"
        print(f"S3 bucket '{bucket_name}' created successfully with ARN: {bucket_arn}")

        update_env_file("S3_BUCKET_ARN", bucket_arn)
        create_bucket_policy()

        return bucket_arn

    except Exception as e:
        print(f"An error occurred while creating the S3 bucket: {e}")
        return None
    

def create_bucket_policy():
    bucket_name = os.getenv("S3_BUCKET_NAME")
    databricks_account_id = os.getenv("DATABRICKS_ACCOUNT_ID")

    if not bucket_name or not databricks_account_id:
        print("Bucket name or Databricks Account ID not set in .env file.")
        return

    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "Grant Databricks Access",
                "Effect": "Allow",
                "Principal": {
                    "AWS": "arn:aws:iam::414351767826:root"
                },
                "Action": [
                    "s3:GetObject",
                    "s3:GetObjectVersion",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket",
                    "s3:GetBucketLocation"
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket_name}/*",
                    f"arn:aws:s3:::{bucket_name}"
                ],
                "Condition": {
                    "StringEquals": {
                        "aws:PrincipalTag/DatabricksAccountId": [
                            databricks_account_id
                        ]
                    }
                }
            }
        ]
    }

    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION")
    )

    policy_json = json.dumps(bucket_policy)

    try:
        s3_client.put_bucket_policy(Bucket=bucket_name, Policy=policy_json)
        print(f"Bucket policy applied to {bucket_name}.")
    except Exception as e:
        print(f"Error applying bucket policy: {e}")

def update_existing_iam_role_policy(bucket_arn):
    iam_client = boto3.client(
        "iam",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )

    role_name = os.getenv("Databricks_Role_Name")
    policy_name = os.getenv("Policy_name")

    try:
        response = iam_client.get_role_policy(
            RoleName=role_name,
            PolicyName=policy_name
        )
        policy_document = response['PolicyDocument']

        updated = False
        for statement in policy_document['Statement']:
            if isinstance(statement["Action"], list) and any("s3:" in action for action in statement["Action"]):
                if "Resource" in statement:
                    resources = statement["Resource"]
                    if not isinstance(resources, list):
                        resources = [resources]

                    # Add new resources
                    if bucket_arn not in resources:
                        resources.append(bucket_arn)
                    if f"{bucket_arn}/*" not in resources:
                        resources.append(f"{bucket_arn}/*")
                    statement["Resource"] = resources
                    updated = True
        if not updated:
            print("No relevant S3 policy statements found to update.")
            return

        # Update the policy document
        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_document)
        )

        print(f"Updated 'Resource' field in policy for IAM role {role_name} to include bucket {bucket_arn}")

    except iam_client.exceptions.NoSuchEntityException:
        print(f"The role {role_name} or policy {policy_name} does not exist.")
    except Exception as e:
        print(f"An error occurred while updating the IAM role policy: {e}")

def update_env_file(key, value):
    """Update the .env file with the given key-value pair."""
    with open(env_path, "a") as f:
        f.write(f"\n{key}={value}\n")

def main():
    # Create the S3 bucket
    bucket_arn = create_s3_bucket()
    if bucket_arn:
        update_existing_iam_role_policy(bucket_arn)
