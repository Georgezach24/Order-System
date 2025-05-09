# --- Import Kafka and MySQL libraries ---
from kafka import KafkaConsumer, KafkaProducer
import json
import mysql.connector

# --- MySQL Database configuration ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'order_system_db'
}

# --- Kafka consumer for login requests ---
consumer = KafkaConsumer(
    'login_requests',                      # Topic to consume login attempts from
    bootstrap_servers='localhost:9092',    # Kafka broker
    auto_offset_reset='earliest',          # Read from beginning if no offset
    group_id='login-checker',              # Consumer group ID
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # Convert JSON to dict
)

# --- Kafka producer for login responses ---
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Convert dict to JSON bytes
)

# --- Function to check credentials against the MySQL users table ---
def check_credentials(username, password):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT password FROM users WHERE username=%s", (username,))
        row = cursor.fetchone()

        if not row:
            return False

        # Compare stored hashed password with incoming password (already hashed by client)
        return password == row[0]
    except Exception as e:
        print("DB error:", e)
        return False

print("Login processor listening on Kafka...")

# --- Main loop: listen for login requests, validate, and respond ---
for msg in consumer:
    data = msg.value
    username = data.get('username')
    password = data.get('password')

    result = check_credentials(username, password)
    status = "success" if result else "failure"

    # Send the result back via Kafka to 'login_responses' topic
    producer.send('login_responses', {
        "username": username,
        "status": status
    })

    print(f"Checked login for {username} → {status}")
