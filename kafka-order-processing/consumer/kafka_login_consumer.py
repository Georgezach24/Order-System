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

# --- Kafka Consumer: listens for login requests from frontend ---
consumer = KafkaConsumer(
    'login_requests',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='login-checker',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# --- Kafka Producer: sends login responses back to the frontend ---
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# --- Credential Verification Function ---
# Compares hashed password with DB record and returns authentication status
def check_credentials(username, password):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT password, isAdmin FROM users WHERE username=%s", (username,))
        row = cursor.fetchone()
        if not row:
            return False, False  # User not found
        return password == row[0], bool(row[1])  # Password match and admin flag
    except Exception as e:
        print("DB error:", e)
        return False, False

print("Login processor listening on Kafka...")

# --- Main Kafka Consumer Loop ---
# Handles login request messages, checks credentials, and responds
for msg in consumer:
    data = msg.value
    username = data.get('username')
    password = data.get('password')       # Assumes password is already hashed
    session_id = data.get('session_id')

    result, is_admin = check_credentials(username, password)
    status = "success" if result else "failure"

    # Send login response to the appropriate Kafka topic
    producer.send('login_responses', {
        "username": username,
        "status": status,
        "isAdmin": is_admin,
        "session_id": session_id
    })

    print(f"Checked login for {username} → {status}, Admin={is_admin} (session {session_id})")
