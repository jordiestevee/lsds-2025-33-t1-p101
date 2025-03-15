#!/usr/bin/env python3
import sys
import time
import json
from confluent_kafka import Producer


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(
            f"Record {msg.key()} produced to {msg.topic()} partition [{msg.partition()}] at offset {msg.offset()}"
        )


def main():
    if len(sys.argv) != 4:
        print(
            "Usage: python3 constant.py <metric_name> <metric_value> <period_seconds>"
        )
        sys.exit(1)

    metric_name = sys.argv[1]
    metric_value = float(sys.argv[2])
    period_seconds = float(sys.argv[3])

    producer_conf = {"bootstrap.servers": "localhost:19092"}
    producer = Producer(producer_conf)

    while True:
        payload = {"value": metric_value}
        producer.produce(
            topic="metrics",
            key=metric_name,
            value=json.dumps(payload),
            callback=delivery_report,
        )
        producer.flush()
        print(f"Published {metric_name}: {payload}")
        time.sleep(period_seconds)


if __name__ == "__main__":
    main()
