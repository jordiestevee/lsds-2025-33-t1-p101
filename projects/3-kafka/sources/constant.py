#!/usr/bin/env python3
import sys
import time
import json
from kafka import KafkaProducer

def main():
    if len(sys.argv) != 4:
        print("Usage: python3 constant.py <metric_name> <metric_value> <period_seconds>")
        sys.exit(1)
    
    metric_name = sys.argv[1]       
    metric_value = float(sys.argv[2])  
    period_seconds = float(sys.argv[3])
    
    producer = KafkaProducer(
        bootstrap_servers='localhost:19092',  
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=str.encode
    )
    
    while True:
        payload = {"value": metric_value}
        
        producer.send(
            topic='metrics',
            key=metric_name,     
            value=payload      
        )
        
        print(f"Published {metric_name}: {payload}")
        
        time.sleep(period_seconds)

if __name__ == "__main__":
    main()
