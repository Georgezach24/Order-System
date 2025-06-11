from kafka import KafkaConsumer, KafkaProducer
import json
import mysql.connector

# --- MySQL Database Configuration ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'order_system_db'
}

# --- Kafka Consumer setup ---
# Listens to all order-related topics
consumer = KafkaConsumer(
    'orders', 'order_updates', 'order_query_requests', 'order_cancellations',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  
    group_id='order-handler',     
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# --- Kafka Producer setup ---
# Used to send query responses back to the client
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  
)

print("Consumer listening on: orders, order_updates, order_query_requests, order_cancellations")

# --- Function to insert a new order into the database ---
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
        print(f"Inserted order #{order['order_id']} for {order['user']}")
    except Exception as e:
        print("Insert Error:", e)

# --- Function to update an existing order ---
def update_order(order):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        # Check if order exists and belongs to the user
        cursor.execute("SELECT * FROM orders WHERE order_id = %s AND user = %s", (order['order_id'], order['user']))
        if not cursor.fetchone():
            print(f"Order #{order['order_id']} not found for {order['user']}")
            return
        # Update the order's fields
        cursor.execute("""
            UPDATE orders SET dt = %s, tm = %s, description = %s WHERE order_id = %s
        """, (
            order['dt'],
            order['tm'],
            order['description'],
            order['order_id']
        ))
        conn.commit()
        print(f"Updated order #{order['order_id']} for {order['user']}")
    except Exception as e:
        print("Update Error:", e)

# --- Function to cancel (delete) an order ---
def cancel_order(order):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        # Check if the order exists and belongs to the user
        cursor.execute("SELECT * FROM orders WHERE order_id = %s AND user = %s", (order['order_id'], order['user']))
        if not cursor.fetchone():
            print(f"Cancel failed: Order #{order['order_id']} not found or unauthorized for {order['user']}")
            return
        # Delete the order
        cursor.execute("DELETE FROM orders WHERE order_id = %s", (order['order_id'],))
        conn.commit()
        print(f"Canceled order #{order['order_id']} for {order['user']}")
    except Exception as e:
        print("Cancel Error:", e)

# --- Function to fetch all orders belonging to a user ---
def fetch_user_orders(user):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        # Select all orders for the given user
        cursor.execute("SELECT order_id, dt, tm, description FROM orders WHERE user = %s", (user,))
        rows = cursor.fetchall()
        # Format the result into a list of dictionaries
        return [
            {
                "order_id": row[0],
                "dt": row[1].strftime('%Y-%m-%d'),  
                "tm": str(row[2]),                  
                "description": row[3]
            }
            for row in rows
        ]
    except Exception as e:
        print("Query Error:", e)
        return []

# --- Main message processing loop ---
for msg in consumer:
    topic = msg.topic     
    data = msg.value      

    # Route the message based on topic
    if topic == 'orders':
        insert_order(data)
    elif topic == 'order_updates':
        update_order(data)
    elif topic == 'order_cancellations':
        cancel_order(data)
    elif topic == 'order_query_requests':
        # Fetch orders and send them back to the 'order_query_responses' topic
        result = fetch_user_orders(data['user'])
        producer.send('order_query_responses', {
            "user": data['user'],
            "orders": result
        })
