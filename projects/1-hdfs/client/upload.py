import requests
import os

# Namenode URL
NAMENODE_URL = "http://localhost:8000"
BLOCK_SIZE = 64 * 1024  # 64 KB

# Ask the user for the file path and filename
file_path = input("Enter the file path: ")
filename = input("Enter the filename: ")

# Print the current working directory and absolute path for debugging
current_directory = os.getcwd()
absolute_path = os.path.abspath(file_path)
print(f"Current working directory: {current_directory}")
print(f"Checking file at: {absolute_path}")

# Check if the file exists
if not os.path.exists(file_path):
    print("File does not exist.")
    exit()

# Get the file size
file_size = os.path.getsize(file_path)
print(f"File size: {file_size} bytes")

# Create the file in the namenode
try:
    payload = {"file_name": filename, "size": file_size}
    print(f"Sending payload to namenode: {payload}")
    create_file_response = requests.post(f"{NAMENODE_URL}/files", json=payload)
    create_file_response.raise_for_status()
    response_data = create_file_response.json()
    print(f"Response from Namenode: {response_data}")
except requests.exceptions.RequestException as e:
    print(f"Failed to create file in namenode: {e}")
    print(f"Response from server: {e.response.text}")
    exit()

# Get the block assignments from the namenode
block_assignments = response_data.get("blocks", [])
if not block_assignments:
    print("No blocks assigned by Namenode. Exiting.")
    exit()

# Upload each block to the assigned datanodes
with open(file_path, "rb") as file:
    for block in block_assignments:
        block_number = block.get("number")  # Updated key
        replicas = block.get("replicas", [])  # Updated key

        if block_number is None or not replicas:
            print(f"Skipping block due to missing data: {block}")
            continue

        # Read the block data
        file.seek(block_number * BLOCK_SIZE)
        block_data = file.read(BLOCK_SIZE)

        # Upload the block to each datanode
        for replica in replicas:
            datanode_url = f"http://{replica['host']}:{replica['port']}"
            upload_url = (
                f"{datanode_url}/files/{filename}/blocks/{block_number}/content"
            )
            try:
                response = requests.put(upload_url, files={"file": block_data})
                response.raise_for_status()
                print(
                    f"Block {block_number} uploaded to {replica['host']}:{replica['port']}"
                )
            except requests.exceptions.RequestException as e:
                print(
                    f"Failed to upload block {block_number} to {replica['host']}:{replica['port']}: {e}"
                )
