from kafka import KafkaConsumer
import json
import mysql.connector

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'order_system_db'
}

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='order-processor',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Order consumer listening on 'orders' topic...")

def insert_order(order):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO orders (user, order_id, dt, tm, description)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            order['user'],
            order['order_id'],
            order['dt'],
            order['tm'],
            order['description']
        ))
        conn.commit()
        print(f"Inserted order for {order['user']} → #{order['order_id']}")
    except Exception as e:
        print("DB Error:", e)

for msg in consumer:
    order = msg.value
    insert_order(order)
