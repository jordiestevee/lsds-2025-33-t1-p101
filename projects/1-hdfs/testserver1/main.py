from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()


class SumRequest(BaseModel):
    x: int
    y: int


@app.post("/sum")
def calculate_sum(request: SumRequest):
    return {"result": request.x + request.y}
