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
    'user_registration_requests',
    bootstrap_servers='localhost:9092',
    group_id='registration-handler',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Listening for registration requests...")

for msg in consumer:
    data = msg.value
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO users (username, password, isAdmin) VALUES (%s, %s, %s)",
            (data['username'], data['password'], data.get('isAdmin', False))
        )
        conn.commit()
        print(f"Registered new user: {data['username']} (Admin: {data.get('isAdmin', False)})")
    except mysql.connector.IntegrityError:
        print(f"User {data['username']} already exists.")
    except Exception as e:
        print("Registration Error:", e)
