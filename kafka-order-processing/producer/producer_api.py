from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json

app = Flask(__name__)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@app.route('/create-order', methods=['POST'])
def create_order():
    try:
        order = request.get_json()
        producer.send('orders', order)
        return jsonify({"status": "Order sent"}), 200
    except Exception as e:
        return jsonify({"status": "Error", "details": str(e)}), 500

if __name__ == '__main__':
    app.run(port=5000)
