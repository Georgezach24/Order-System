from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def create_order():
    return {
        "order_id": random.randint(1000, 9999),
        "user_id": random.randint(1, 100),
        "items": ["item1", "item2", "item3"],
        "total": round(random.uniform(10.0, 500.0), 2),
        "created_at": time.strftime('%Y-%m-%d %H:%M:%S')
    }

while True:
    order = create_order()
    print(f"Producing order: {order}")
    producer.send('orders', order)
    time.sleep(1)
