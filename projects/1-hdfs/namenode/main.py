from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import json
from pathlib import Path

app = FastAPI()
# Paths to config.json and files.json
CONFIG_PATH = Path(__file__).parent / "config.json"
FILES_PATH = Path(__file__).parent / "files.json"


@app.get("/datanodes")
def get_datanodes():
    """
    Returns the list of datanodes from config.json.
    """
    return {"datanodes": config["datanodes"]}


# Load config.json at startup
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    config = json.load(f)


# Helper functions to read/write files.json
def read_files_json():
    with open(FILES_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def write_files_json(data):
    with open(FILES_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)


# Request body schema for file creation
class CreateFileRequest(BaseModel):
    file_name: str
    size: int


@app.post("/files")
def create_file(req: CreateFileRequest):
    """
    Create a new file in the namenode and assign blocks to datanodes.
    """
    file_name = req.file_name
    file_size = req.size

    # 1. Read data from config.json
    datanodes = config["datanodes"]
    replication_factor = config["replication_factor"]
    block_size = config["block_size"]

    # 2. Compute number of blocks
    num_blocks = (file_size + block_size - 1) // block_size  # ceiling division

    # 3. Assign blocks to datanodes using modulo-based policy
    blocks_metadata = []
    for block_number in range(num_blocks):
        start = block_number * block_size
        end = min(start + block_size, file_size)
        block_size_actual = end - start

        datanode_index = block_number % len(datanodes)  # Modulo-based assignment
        replicas = []
        for r in range(replication_factor):
            replica_index = (datanode_index + r) % len(datanodes)
            replicas.append(datanodes[replica_index])

        blocks_metadata.append(
            {"number": block_number, "size": block_size_actual, "replicas": replicas}
        )

    # 4. Read existing files.json
    files_data = read_files_json()

    # Check if the file already exists
    if file_name in files_data:
        raise HTTPException(status_code=400, detail="File already exists")

    # 5. Create new file metadata
    file_metadata = {
        "file_name": file_name,
        "size": file_size,
        "blocks": blocks_metadata,
    }

    # 6. Write to files.json
    files_data[file_name] = file_metadata
    write_files_json(files_data)

    return file_metadata


@app.get("/files/{filename}")
def get_file_metadata(filename: str):
    """
    Endpoint to retrieve metadata for a file by its filename.
    """
    # Read the existing files metadata
    files_data = read_files_json()

    # Check if the requested file exists
    if filename not in files_data:
        raise HTTPException(status_code=404, detail="File not found")

    # Return the metadata for the requested file
    return files_data[filename]
