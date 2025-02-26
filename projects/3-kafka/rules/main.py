import uuid
import json
from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaProducer

app = FastAPI()

KAFKA_BROKER = "kafka-1:9092"
KAFKA_TOPIC = "rules"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

class Rule(BaseModel):
    name: str
    condition: str
    action: str

@app.post("/rules")
async def create_rule(rule: Rule):
    rule_id = str(uuid.uuid4())
    rule_data = {
        "id": rule_id,
        "name": rule.name,
        "condition": rule.condition,
        "action": rule.action,
    }

    producer.send(KAFKA_TOPIC, rule_data)
    producer.flush()

    return {"message": "Rule created", "rule": rule_data}