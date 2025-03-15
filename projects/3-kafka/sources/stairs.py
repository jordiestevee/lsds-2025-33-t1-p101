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
    if len(sys.argv) != 6:
        print(
            "Usage: python3 stairs.py <metric_name> <start_value> <end_value> <step> <period_seconds>"
        )
        sys.exit(1)

    metric_name = sys.argv[1]
    start_value = float(sys.argv[2])
    end_value = float(sys.argv[3])
    step = float(sys.argv[4])
    period_seconds = float(sys.argv[5])

    producer_conf = {"bootstrap.servers": "localhost:19092"}
    producer = Producer(producer_conf)

    current_value = start_value

    while True:
        payload = {"value": current_value}

        producer.produce(
            topic="metrics",
            key=metric_name,
            value=json.dumps(payload),
            callback=delivery_report,
        )
        producer.flush()

        print(f"Published {metric_name}: {payload}")

        current_value += step

        if current_value > end_value:
            current_value = start_value

        time.sleep(period_seconds)


if __name__ == "__main__":
    main()
