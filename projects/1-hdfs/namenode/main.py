from fastapi import FastAPI
import json
from pathlib import Path

app = FastAPI()

# Endpoint to get datanodes
@app.get("/datanodes")
async def get_datanodes():
    # Load datanodes from config.json
    config_path = Path(__file__).parent / "config.json"
    with open(config_path, "r") as f:
        config = json.load(f)
    return {"datanodes": config["datanodes"]}
