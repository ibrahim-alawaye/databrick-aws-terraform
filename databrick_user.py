import json
import requests
import boto3
import csv

# Databricks Configuration
base_url = "https://<databricks-instance>.databricks.com"
access_token = "YOUR_HARDCODED_ACCESS_TOKEN"

# Function to parse S3 URL
def parse_s3_url(url):
    if url.startswith("s3://"):
        url = url[5:]
    bucket, key = url.split("/", 1)
    return bucket, key

# Function to load configuration from S3
def load_config_from_s3(s3_url):
    bucket_name, object_key = parse_s3_url(s3_url)
    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    content = response["Body"].read().decode("utf-8").splitlines()
    reader = csv.DictReader(content)
    return list(reader)

# Function to make API requests to Databricks
def api_request(method, endpoint, data=None):
    url = f"{base_url}/api/2.0/{endpoint}"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    response = requests.request(method, url, headers=headers, json=data)
    
    if response.status_code not in (200, 201):
        print(f"Error {response.status_code}: {response.text}")
        return None
    return response.json()

# Function to create a group in Databricks
def create_group(group_name):
    data = {"displayName": group_name}
    return api_request("POST", "preview/scim/v2/Groups", data)

# Function to get a user by email from Databricks
def get_user(email):
    response = api_request("GET", f"preview/scim/v2/Users?filter=email eq '{email}'")
    if response and "Resources" in response and len(response["Resources"]) > 0:
        return response["Resources"][0]  # Return the first matching user
    return None

# Function to create a new user in Databricks
def create_user(first_name, last_name, email):
    data = {
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
        "userName": email,
        "name": {"givenName": first_name, "familyName": last_name},
        "emails": [{"value": email, "primary": True}]
    }
    return api_request("POST", "preview/scim/v2/Users", data)

# Function to add a user to a group
def add_user_to_group(user_id, group_id):
    data = {"members": [{"value": user_id}]}
    return api_request("PATCH", f"preview/scim/v2/Groups/{group_id}", data)

# Function to extract the first and last name from an email address
def extract_name_from_email(email):
    name_part = email.split("@")[0]
    parts = name_part.split(".")
    first_name = parts[0].capitalize()
    last_name = parts[1].capitalize() if len(parts) > 1 else ""
    return first_name, last_name

# Main function to create a Databricks group and add users from a config file
def create_databricks_user_group_and_add_users_from_file(s3_url):
    config = load_config_from_s3(s3_url)
    group_name = config[0]["Group Name"]
    
    # Create group or get existing group
    group = create_group(group_name)
    if group:
        group_id = group["id"]
        print(f"Group '{group_name}' created with ID: {group_id}")
    else:
        print(f"Group '{group_name}' already exists or could not be created.")
        return

    # Process each user in the config
    for user_config in config:
        email = user_config["User Email"]
        
        # Check if the user already exists
        existing_user = get_user(email)
        if existing_user:
            print(f"User '{email}' already exists. Skipping creation.")
            user_id = existing_user["id"]
        else:
            # Create the user if they do not exist
            first_name, last_name = extract_name_from_email(email)
            new_user = create_user(first_name, last_name, email)
            if new_user:
                user_id = new_user["id"]
                print(f"User '{email}' created with ID: {user_id}")
            else:
                print(f"Failed to create user '{email}'.")
                continue
        
        # Add the user to the group
        add_user_to_group(user_id, group_id)
        print(f"User '{email}' added to group '{group_name}'.")

# Lambda handler function
def lambda_handler(event, context):
    # Get S3 URL from the event
    s3_url = event.get("s3_url")
    if not s3_url:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "S3 URL not provided in the event."})
        }
    
    try:
        create_databricks_user_group_and_add_users_from_file(s3_url)
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "User processing completed successfully."})
        }
    except Exception as e:
        print(f"An error occurred: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
