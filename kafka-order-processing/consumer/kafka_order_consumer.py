from kafka import KafkaConsumer, KafkaProducer
import json
import mysql.connector

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'order_system_db'
}

consumer = KafkaConsumer(
    'orders', 'order_updates', 'order_query_requests', 'order_cancellations',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='order-handler',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Consumer listening on Kafka...")

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

def update_order(order):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM orders WHERE order_id = %s AND user = %s", (order['order_id'], order['user']))
        if not cursor.fetchone():
            print(f"Order #{order['order_id']} not found for {order['user']}")
            return
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

def cancel_order(order):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM orders WHERE order_id = %s AND user = %s", (order['order_id'], order['user']))
        if not cursor.fetchone():
            print(f"Cancel failed: Order #{order['order_id']} not found or unauthorized for {order['user']}")
            return
        cursor.execute("DELETE FROM orders WHERE order_id = %s", (order['order_id'],))
        conn.commit()
        print(f"Canceled order #{order['order_id']} for {order['user']}")
    except Exception as e:
        print("Cancel Error:", e)

def fetch_user_orders(user=None, is_admin=False):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        if is_admin and (user is None or user == "*"):
            cursor.execute("SELECT user, order_id, dt, tm, description FROM orders")
            rows = cursor.fetchall()
            print(f"Admin fetched all orders")
            return [
                {
                    "user": row[0],
                    "order_id": row[1],
                    "dt": row[2].strftime('%Y-%m-%d'),
                    "tm": str(row[3]),
                    "description": row[4]
                }
                for row in rows
            ]
        else:
            cursor.execute("SELECT order_id, dt, tm, description FROM orders WHERE user = %s", (user,))
            rows = cursor.fetchall()
            print(f"Fetched orders for user {user}")
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

for msg in consumer:
    topic = msg.topic
    data = msg.value

    if topic == 'orders':
        insert_order(data)
    elif topic == 'order_updates':
        update_order(data)
    elif topic == 'order_cancellations':
        cancel_order(data)
    elif topic == 'order_query_requests':
        result = fetch_user_orders(
            user=data.get('user'),
            is_admin=data.get('isAdmin', False)
        )
        producer.send('order_query_responses', {
            "user": data.get('user'),
            "orders": result
        })
