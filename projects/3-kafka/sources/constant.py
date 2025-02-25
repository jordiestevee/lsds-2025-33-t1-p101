import sys
import time
import json
from kafka import KafkaProducer

if len(sys.argv) != 4:
    print("Usage: python3 constant.py <metric_name> <metric_value> <period_seconds>")
    sys.exit(1)

metric_name = sys.argv[1]
metric_value = sys.argv[2]
period_seconds = int(sys.argv[3])

topic = "metrics"
producer = KafkaProducer(
    bootstrap_servers='kafka-1:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    message = {"value": int(metric_value)}
    producer.send(topic, key=metric_name.encode('utf-8'), value=message)
    print(f"Published: {metric_name}: {message}")
    time.sleep(period_seconds)
