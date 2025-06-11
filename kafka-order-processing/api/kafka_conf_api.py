from flask import Flask, request, jsonify
from kafka import KafkaProducer
import mysql.connector
import json
import hashlib

app = Flask(__name__)

# Kafka producer for publishing events/messages
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# MySQL configuration (not used in this file, possibly used by consumers)
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'order_system_db'
}

# --- ROUTES ---

# Handle login request, publish credentials and session_id to Kafka
@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    session_id = data.get('session_id')

    if not session_id:
        return jsonify({"error": "Missing session_id"}), 400

    event = {
        "username": username,
        "password": password,
        "session_id": session_id
    }
    producer.send('login_requests', event)
    return jsonify({"status": "pending"}), 200

# Publish new order to Kafka
@app.route('/create-order', methods=['POST'])
def create_order():
    order = request.get_json()
    producer.send('orders', order)
    return jsonify({"status": "Order created"}), 200

# Publish order update to Kafka
@app.route('/update-order', methods=['POST'])
def update_order():
    order = request.get_json()
    producer.send('order_updates', order)
    return jsonify({"status": "Order updated"}), 200

# Publish order cancellation to Kafka
@app.route('/cancel-order', methods=['POST'])
def cancel_order():
    order = request.get_json()
    producer.send('order_cancellations', order)
    return jsonify({"status": "Order cancelled"}), 200

# Request to fetch orders for a user (or all users if admin)
@app.route('/view_orders', methods=['POST'])
def view_orders():
    data = request.get_json()
    username = data.get('user')
    if not username:
        return jsonify({"error": "Missing 'user'"}), 400

    producer.send('order_query_requests', {"user": username})
    return jsonify({"status": "Order request sent"}), 200

# Handle user registration, publish user data to Kafka
@app.route('/register-user', methods=['POST'])
def register_user():
    data = request.get_json()
    if not data.get('username') or not data.get('password'):
        return jsonify({"error": "Missing username or password"}), 400

    event = {
        "username": data['username'],
        "password": data['password'],  # Password is expected to be pre-hashed
        "isAdmin": data.get('isAdmin', False)
    }
    producer.send('user_registration_requests', event)
    return jsonify({"status": "Registration request sent"}), 200

# Start the Flask app on port 5000
if __name__ == '__main__':
    app.run(port=5000)
