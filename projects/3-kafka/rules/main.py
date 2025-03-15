import uuid
import json
from fastapi import FastAPI
from pydantic import BaseModel
from confluent_kafka import Producer


class RuleIn(BaseModel):
    metric_name: str
    operator: str
    threshold: float


class RuleOut(BaseModel):
    id: str
    metric_name: str
    operator: str
    threshold: float


app = FastAPI()

producer_config = {"bootstrap.servers": "kafka-1:9092"}
producer = Producer(producer_config)

rules_store = {}


@app.post("/rules", response_model=RuleOut)
def create_rule(rule_in: RuleIn):
    rule_id = str(uuid.uuid4())
    rule_dict = {
        "id": rule_id,
        "metric_name": rule_in.metric_name,
        "operator": rule_in.operator,
        "threshold": rule_in.threshold,
    }
    rules_store[rule_id] = rule_dict
    producer.produce(topic="rules", key=rule_id, value=json.dumps(rule_dict))
    producer.flush()
    return rule_dict


@app.delete("/rules/{rule_id}")
def delete_rule(rule_id: str):
    if rule_id in rules_store:
        del rules_store[rule_id]
    producer.produce(topic="rules", key=rule_id, value=None)
    producer.flush()
    return {"id": rule_id, "deleted": True}


@app.get("/rules", response_model=list[RuleOut])
def list_rules():
    return list(rules_store.values())


@app.get("/health")
def health():
    return {"status": "ok"}
