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


@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    event = {
        "username": username,
        "password": password
    }
    producer.send('login_requests', event)

    return jsonify({"status": "pending"}), 200


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
