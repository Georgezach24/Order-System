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

# --- Kafka Consumer Setup ---
print("Starting login consumer...")

consumer = KafkaConsumer(
    'login_requests',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='login-checker',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Connected to Kafka. Waiting for login requests...")

# --- Kafka Producer Setup ---
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# --- Credential Verification Function ---
def check_credentials(username, password):
    try:
        print(f"Checking DB for user: {username}")
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',
            database='order_system_db',
        )
        cursor = conn.cursor()
        cursor.execute("SELECT password, isAdmin FROM users WHERE username=%s", (username,))
        row = cursor.fetchone()
        if not row:
            print("User not found in DB.")
            return False, False
        print("DB user found.")
        return password == row[0], bool(row[1])
    except Exception as e:
        print("DB error:", e)
        return False, False

# --- Main Kafka Consumer Loop ---
for msg in consumer:
    try:
        data = msg.value
        print("Received login request:", data)

        username = data.get('username')
        password = data.get('password')
        session_id = data.get('session_id')

        if not username or not password or not session_id:
            print("Incomplete login message:", data)
            continue

        result, is_admin = check_credentials(username, password)
        status = "success" if result else "failure"

        response = {
            "username": username,
            "status": status,
            "isAdmin": is_admin,
            "session_id": session_id
        }

        producer.send('login_responses', response)
        print(f"Checked login for {username} → {status}, Admin={is_admin}, Session={session_id}")

    except Exception as e:
        print("Error processing login message:", e)
