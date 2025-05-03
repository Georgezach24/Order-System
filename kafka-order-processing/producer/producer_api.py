from flask import Flask, request, jsonify
from kafka import KafkaProducer
import mysql.connector
import json
import hashlib

app = Flask(__name__)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'order_system_db'
}

def check_user(username, password):
    hashed = hashlib.sha256(password.encode()).hexdigest()
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users WHERE username=%s AND password=%s", (username, hashed))
        return cursor.fetchone() is not None
    except Exception as e:
        print("DB error:", e)
        return False

@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    success = check_user(username, password)
    
    event = {
        "event": "login_attempt",
        "username": username,
        "status": "success" if success else "failure"
    }
    producer.send('login_events', event)

    if success:
        return jsonify({"status": "ok"}), 200
    return jsonify({"status": "unauthorized"}), 401

@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    if users_db.get(username) == password:
        return jsonify({"status": "ok"}), 200
    return jsonify({"status": "unauthorized"}), 401

@app.route('/create-order', methods=['POST'])
def create_order():
    order = request.get_json()
    producer.send('orders', order)
    return jsonify({"status": "Order created"}), 200

@app.route('/update-order', methods=['POST'])
def update_order():
    order = request.get_json()
    producer.send('order_updates', order)
    return jsonify({"status": "Order updated"}), 200

@app.route('/cancel-order', methods=['POST'])
def cancel_order():
    order = request.get_json()
    producer.send('order_cancellations', order)
    return jsonify({"status": "Order cancelled"}), 200

if __name__ == '__main__':
    app.run(port=5000)
