#!/usr/bin/env python3
import sys
import time
import json
from kafka import KafkaProducer

def main():
    if len(sys.argv) != 6:
        print("Usage: python3 stairs.py <metric_name> <start_value> <end_value> <step> <period_seconds>")
        sys.exit(1)
    
    metric_name = sys.argv[1]       
    start_value = float(sys.argv[2])
    end_value = float(sys.argv[3])  
    step = float(sys.argv[4])       
    period_seconds = float(sys.argv[5]) 
    
    producer = KafkaProducer(
        bootstrap_servers='localhost:19092', 
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=str.encode
    )
    
    current_value = start_value
    
    while True:
        payload = {"value": current_value}
        
        producer.send(
            topic='metrics',
            key=metric_name,         
            value=payload            
        )
        
        print(f"Published {metric_name}: {payload}")
        
        current_value += step
        
        if current_value > end_value:
            current_value = start_value
        
        time.sleep(period_seconds)

if __name__ == "__main__":
    main()
