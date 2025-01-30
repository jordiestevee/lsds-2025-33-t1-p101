import requests
import os

# Namenode URL
namenode_url = "http://localhost:8000"

# Ask the user for the file path and filename
file_path = input("Enter the file path: ")
filename = input("Enter the filename: ")

# Check if the file exists
if not os.path.exists(file_path):
    print("File does not exist.")
    exit()

# Get the file size
file_size = os.path.getsize(file_path)

# Create the file in the namenode
try:
    create_file_response = requests.post(
        f"{namenode_url}/files",
        json={"filename": filename, "size": file_size}
    )
    create_file_response.raise_for_status()
except requests.exceptions.RequestException as e:
    print(f"Failed to create file in namenode: {e}")
    exit()

# Get the block assignments from the namenode
block_assignments = create_file_response.json()["blocks"]

# Upload each block to the assigned datanodes
with open(file_path, "rb") as file:
    for block in block_assignments:
        block_number = block["block_number"]
        datanodes = block["datanodes"]

        # Read the block data
        file.seek(block_number * 64 * 1024)  # 64 KB block size
        block_data = file.read(64 * 1024)

        # Upload the block to each datanode
        for datanode in datanodes:
            datanode_url = f"http://{datanode['host']}:{datanode['port']}"
            upload_url = f"{datanode_url}/files/{filename}/blocks/{block_number}/content"
            try:
                response = requests.put(upload_url, files={"file": block_data})
                response.raise_for_status()
                print(f"Block {block_number} uploaded to {datanode['host']}:{datanode['port']}")
            except requests.exceptions.RequestException as e:
                print(f"Failed to upload block {block_number} to {datanode['host']}:{datanode['port']}: {e}")
