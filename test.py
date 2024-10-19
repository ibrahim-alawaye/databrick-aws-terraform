import json
import csv
import requests


base_url="https://dbc-e52-icks.com"
access_token="dapi3c14ceb8d9"


def load_config_from_csv(file_path):
    with open(file_path, mode='r') as file:
        content = csv.DictReader(file)
        return list(content)

def api_request(method, endpoint, data=None):
    url = f"{base_url}/api/2.0/{endpoint}"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    print(f"Request Method: {method}, URL: {url}, Data: {data}")  
    response = requests.request(method, url, headers=headers, json=data)

    if response.status_code not in (200, 201):
        print(f"Error {response.status_code}: {response.text}")
        return None
    return response.json()


def get_existing_group(group_name):
    response = api_request("GET", f"preview/scim/v2/Groups?filter=displayName eq '{group_name}'")
    if response and "Resources" in response and len(response["Resources"]) > 0:
        return response["Resources"][0] 
    return None

def create_group(group_name):
    existing_group = get_existing_group(group_name)
    if existing_group:
        print(f"Group '{group_name}' already exists with ID: {existing_group['id']}")
        return existing_group 

    data = {"displayName": group_name}
    new_group_response = api_request("POST", "preview/scim/v2/Groups", data)
    
    if new_group_response:
        print(f"Group '{group_name}' created with ID: {new_group_response['id']}")
    return new_group_response 

def get_user(email):
    response = api_request("GET", f"preview/scim/v2/Users?filter=email eq '{email}'")
    if response and "Resources" in response and len(response["Resources"]) > 0:
        return response["Resources"][0]  # Return the first matching user
    return None

def create_user(first_name, last_name, email):
    data = {
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
        "userName": email,
        "name": {"givenName": first_name, "familyName": last_name},
        "emails": [{"value": email, "primary": True}]
    }
    return api_request("POST", "preview/scim/v2/Users", data)


def add_user_to_group(user_id, group_id):
    data = {"members": [{"value": user_id}]}
    print(f"Adding user {user_id} to group {group_id} with data: {data}")
    response = api_request("PATCH", f"preview/scim/v2/Groups/{group_id}", data)

    if response is None:
        print(f"Failed to add user {user_id} to group {group_id}.")
    else:
        print(f"User {user_id} added to group {group_id}. Response: {response}")

def extract_name_from_email(email):
    name_part = email.split("@")[0]
    parts = name_part.split(".")
    first_name = parts[0].capitalize()
    last_name = parts[1].capitalize() if len(parts) > 1 else ""
    return first_name, last_name

def create_databricks_user_group_and_add_users_from_file(file_path):
    config = load_config_from_csv(file_path)
    group_name = config[0]["Group Name"]

    group = create_group(group_name)
    if not group:
        print(f"Failed to retrieve or create group '{group_name}'.")
        return
    
    group_id = group["id"]

    for user_config in config:
        email = user_config["User Email"]

        existing_user = get_user(email)
        if existing_user:
            print(f"User '{email}' already exists. Skipping creation.")
            user_id = existing_user["id"]
        else:
    
            first_name, last_name = extract_name_from_email(email)
            new_user = create_user(first_name, last_name, email)
            if new_user:
                user_id = new_user["id"]
                print(f"User '{email}' created with ID: {user_id}")
            else:
                print(f"Failed to create user '{email}'.")
                continue

        add_user_to_group(user_id, group_id)
    
if __name__ == "__main__":
    local_csv_file_path = "test.csv" 
    create_databricks_user_group_and_add_users_from_file(local_csv_file_path)
