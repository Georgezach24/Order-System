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
    'orders', 'order_updates', 'order_query_requests',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='order-handler',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Consumer listening to: orders, order_updates, order_query_requests")

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
        print(f"✅ Inserted order #{order['order_id']} for {order['user']}")
    except Exception as e:
        print("❌ Insert Error:", e)

def update_order(order):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM orders WHERE order_id = %s AND user = %s
        """, (order['order_id'], order['user']))
        if not cursor.fetchone():
            print(f"⚠️ Order #{order['order_id']} not found for {order['user']}")
            return
        cursor.execute("""
            UPDATE orders
            SET dt = %s, tm = %s, description = %s
            WHERE order_id = %s
        """, (
            order['dt'],
            order['tm'],
            order['description'],
            order['order_id']
        ))
        conn.commit()
        print(f"🔁 Updated order #{order['order_id']} for {order['user']}")
    except Exception as e:
        print("❌ Update Error:", e)

def fetch_user_orders(user):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT order_id, dt, tm, description FROM orders WHERE user = %s
        """, (user,))
        orders = cursor.fetchall()
        return [
            {
                "order_id": row[0],
                "dt": row[1].strftime('%Y-%m-%d'),
                "tm": str(row[2]),  
                "description": row[3]
            }
            for row in orders
        ]
    except Exception as e:
        print("❌ Query Error:", e)
        return []

for msg in consumer:
    topic = msg.topic
    data = msg.value

    if topic == 'orders':
        insert_order(data)

    elif topic == 'order_updates':
        update_order(data)

    elif topic == 'order_query_requests':
        user = data.get('user')
        print(f"📨 Order history request from {user}")
        result = fetch_user_orders(user)
        response = {
            "user": user,
            "orders": result
        }
        producer.send('order_query_responses', response)
