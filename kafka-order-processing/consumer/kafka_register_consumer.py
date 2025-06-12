from kafka import KafkaConsumer, KafkaProducer
import json
import mysql.connector

# --- Database Configuration ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'order_system_db'
}

# --- Kafka Consumer Setup ---
# Listens to the 'user_registration_requests' topic and processes each message
print("Starting user registration consumer...")

consumer = KafkaConsumer(
    'user_registration_requests',
    bootstrap_servers='localhost:9092',
    group_id='registration-handler',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Connected to Kafka. Listening for registration requests...")

# --- Message Processing Loop ---
# For every registration message received, try inserting a new user into MySQL
for msg in consumer:
    data = msg.value
    print("Received registration request:", data)
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
