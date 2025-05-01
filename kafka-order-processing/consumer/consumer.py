from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # start from the beginning of the topic
    group_id='order-processor',    # consumer group name
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Consumer started. Waiting for messages...\n")

for msg in consumer:
    print(f"Consumed order: {msg.value}")
