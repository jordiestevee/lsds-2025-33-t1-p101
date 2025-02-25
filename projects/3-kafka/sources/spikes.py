#!/usr/bin/env python3
import sys
import time
import json
from kafka import KafkaProducer

def main():
    if len(sys.argv) != 6:
        print("Usage: python3 spikes.py <metric_name> <low_value> <spike_value> <period_seconds> <frequency>")
        sys.exit(1)

    metric_name = sys.argv[1]       
    low_value = float(sys.argv[2])  
    spike_value = float(sys.argv[3])
    period_seconds = float(sys.argv[4])
    frequency = int(sys.argv[5])   
    
    producer = KafkaProducer(
        bootstrap_servers='localhost:19092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=str.encode
    )
    
    msg_count = 0
    while True:
        if msg_count % frequency == 0:
            value = spike_value
        else:
            value = low_value
        
        payload = {"value": value}
        
        producer.send(
            topic='metrics',
            key=metric_name,           
            value=payload              
        )
        
        print(f"Published {metric_name}: {payload}")
        
        msg_count += 1
        time.sleep(period_seconds)

if __name__ == "__main__":
    main()
