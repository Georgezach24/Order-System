from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  
    group_id='order-processor',    
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Consumer started. Waiting for messages...\n")

for msg in consumer:
    print(f"Consumed order: {msg.value}")


consumer = KafkaConsumer(
    'login_events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='audit-service',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for msg in consumer:
    event = msg.value
    print(f"[LOGIN EVENT] {event['username']} - {event['status']}")