from confluent_kafka import Consumer, KafkaError
import requests
import threading
import time

rules_view = {}

# Discord webhook URL
discord_webhook_url = "https://discord.com/api/webhooks/1346824318666936351/U-quGFO3NXPyhL_psKmDEyu20NQf1s7dsYhnqw_x2iaIWGfhvPIn7Fkoe2X02u5LQ7pj"

# Function to consume rules and update the materialized view
import json


def consume_rules():
    consumer = Consumer(
        {
            "bootstrap.servers": "kafka-1:9092",
            "group.id": "alarms-rules-consumer",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(["rules"])

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        rule_id = msg.key().decode("utf-8")
        rule_value = json.loads(msg.value().decode("utf-8"))
        rules_view[rule_id] = rule_value
        print(f"Updated rule: {rule_id} -> {rule_value}")


# Function to consume metrics and check for triggered rules
def consume_metrics():
    consumer = Consumer(
        {
            "bootstrap.servers": "kafka-1:9092",
            "group.id": "alarms-metrics-consumer",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(["metrics"])

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        metric_value = msg.value().decode("utf-8")
        for rule_id, rule_condition in rules_view.items():
            if evaluate_rule(rule_condition, metric_value):
                send_alarm(rule_id, metric_value)


#  Evaluate if a rule is triggered based on the metric value.
def evaluate_rule(rule_condition, metric_value):
    try:
        # Extract the metric name and value from the metric_value string
        metric_name, value = metric_value.split("=")
        value = float(value)

        # Check if the metric name matches the rule condition
        if metric_name != rule_condition["metric_name"]:
            return False

        # Evaluate the rule based on the operator
        operator = rule_condition["operator"]
        threshold = float(rule_condition["threshold"])

        if operator == ">" and value > threshold:
            return True
        elif operator == "<" and value < threshold:
            return True
        elif operator == "==" and value == threshold:
            return True
        elif operator == ">=" and value >= threshold:
            return True
        elif operator == "<=" and value <= threshold:
            return True
        else:
            return False
    except Exception as e:
        print(f"Error evaluating rule: {e}")
        return False


# Sending alarms to discord (function)
def send_alarm(rule_id, metric_value):
    message = f"Rule {rule_id} triggered! Metric: {metric_value}"
    payload = {"content": message}
    response = requests.post(discord_webhook_url, json=payload)
    print(
        f"Sent alarm: {message}, Discord response: {response.status_code}, {response.text}"
    )


threading.Thread(target=consume_rules, daemon=True).start()

threading.Thread(target=consume_metrics, daemon=True).start()

while True:
    time.sleep(1)
