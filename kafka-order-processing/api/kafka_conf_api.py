# --- Import required libraries ---
from flask import Flask, request, jsonify
from kafka import KafkaProducer
import mysql.connector
import json
import hashlib

# --- Initialize Flask app ---
app = Flask(__name__)

# --- Kafka producer setup ---
producer = KafkaProducer(
    bootstrap_servers='localhost:9092', 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# --- Database config (used optionally if DB check is added) ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'order_system_db'
}

# --- Route to receive login requests ---
@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    # Send login attempt to Kafka topic for validation
    event = {
        "username": username,
        "password": password
    }
    producer.send('login_requests', event)

    return jsonify({"status": "pending"}), 200  

# --- Route to handle new order creation ---
@app.route('/create-order', methods=['POST'])
def create_order():
    order = request.get_json()
    producer.send('orders', order)  
    return jsonify({"status": "Order created"}), 200

# --- Route to handle order updates ---
@app.route('/update-order', methods=['POST'])
def update_order():
    order = request.get_json()
    producer.send('order_updates', order)  
    return jsonify({"status": "Order updated"}), 200

# --- Route to handle order cancellations ---
@app.route('/cancel-order', methods=['POST'])
def cancel_order():
    order = request.get_json()
    producer.send('order_cancellations', order) 
    return jsonify({"status": "Order cancelled"}), 200

@app.route('/view_orders', methods=['POST'])
def view_orders():
    data = request.get_json()
    username = data.get('user')
    if not username:
        return jsonify({"error": "Missing 'user'"}), 400

    producer.send('order_query_requests', {"user": username})
    return jsonify({"status": "Order request sent"}), 200

# --- Start the Flask server on port 5000 ---
if __name__ == '__main__':
    app.run(port=5000)
