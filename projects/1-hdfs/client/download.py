import requests

# Namenode URL
namenode_url = "http://localhost:8000"

# Ask the user for the filename and destination path
filename = input("Enter the filename: ")
destination_path = input("Enter the destination path: ")

# Retrieve file metadata from the namenode
try:
    file_metadata_response = requests.get(f"{namenode_url}/files/{filename}")
    file_metadata_response.raise_for_status()
except requests.exceptions.RequestException as e:
    print(f"File not found in namenode: {e}")
    exit()

file_metadata = file_metadata_response.json()
blocks = file_metadata["blocks"]

# Download each block and write to the destination file
with open(destination_path, "wb") as file:
    for block in blocks:
        block_number = block["block_number"]
        datanodes = block["datanodes"]

        # Try downloading from each datanode until successful
        for datanode in datanodes:
            datanode_url = f"http://{datanode['host']}:{datanode['port']}"
            download_url = f"{datanode_url}/files/{filename}/blocks/{block_number}/content"
            try:
                response = requests.get(download_url)
                response.raise_for_status()
                file.write(response.content)
                print(f"Block {block_number} downloaded from {datanode['host']}:{datanode['port']}")
                break
            except requests.exceptions.RequestException as e:
                print(f"Failed to download block {block_number} from {datanode['host']}:{datanode['port']}: {e}")
        else:
            print(f"Failed to download block {block_number}")

print(f"File downloaded to {destination_path}")