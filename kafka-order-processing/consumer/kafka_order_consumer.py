# --- Import required libraries ---
from kafka import KafkaConsumer
import json
import mysql.connector

# --- Database connection configuration ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',  # Default for XAMPP or local MySQL
    'database': 'order_system_db'
}

# --- Kafka Consumer setup to listen for new orders ---
consumer = KafkaConsumer(
    'orders',                             # Topic to listen to
    bootstrap_servers='localhost:9092',   # Kafka server address
    auto_offset_reset='earliest',         # Read from beginning if no offset
    group_id='order-processor',           # Consumer group ID
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # Convert JSON to Python dict
)

print("Order consumer listening on 'orders' topic...")

# --- Insert received order into MySQL database ---
def insert_order(order):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO orders (user, order_id, dt, tm, description)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            order['user'],         # Username who created the order
            order['order_id'],     # Random order ID
            order['dt'],           # Date string (YYYY-MM-DD)
            order['tm'],           # Time string (HH:MM:SS)
            order['description']   # Text description of the order
        ))
        conn.commit()
        print(f"Inserted order for {order['user']} → #{order['order_id']}")
    except Exception as e:
        print("DB Error:", e)

# --- Main loop: consume messages and insert them ---
for msg in consumer:
    order = msg.value  # Kafka message value (already parsed as dict)
    insert_order(order)
