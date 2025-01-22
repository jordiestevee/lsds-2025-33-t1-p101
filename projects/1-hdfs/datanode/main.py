from fastapi import FastAPI, UploadFile, HTTPException
import os

app = FastAPI()

# Data directory
DATA_DIR = "storage"

# Ensure the storage directory exists
os.makedirs(DATA_DIR, exist_ok=True)

@app.put("/files/{filename}/blocks/{block_number}/content")
async def upload_block(filename: str, block_number: int, file: UploadFile):
    # Create a directory for the file
    file_dir = os.path.join(DATA_DIR, filename)
    os.makedirs(file_dir, exist_ok=True)

    # Define the block file path
    block_path = os.path.join(file_dir, str(block_number))

    # Write the uploaded file's content to the block file
    try:
        with open(block_path, "wb") as f:
            content = await file.read()
            f.write(content)
        return {"message": f"Block {block_number} of file {filename} stored successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))